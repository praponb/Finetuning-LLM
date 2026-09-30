WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE (requester_id <= 16 OR opened_at < TIMESTAMP '2025-03-02 08:00:00')
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref
    FROM cleaned_tickets
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(csat) OVER w_priority_opened_at_month AS priority_opened_at_month_sum_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority, MONTH(opened_at))) AS distinct_agent_id_in_priority_opened_at_month,
        LAST_VALUE(requester_id_bucket) IGNORE NULLS OVER (PARTITION BY csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id_bucket,
        LAG(closed_at, 3) OVER w_opened_at_month_csat_by_csat AS prev3_closed_at,
        COUNT(*) OVER (PARTITION BY priority, MONTH(opened_at)) AS rows_in_priority_opened_at_month,
        RANK() OVER (PARTITION BY csat ORDER BY csat) AS csat_rank
    FROM tagged
    WINDOW w_priority_opened_at_month AS (PARTITION BY priority, MONTH(opened_at)), w_opened_at_month_csat_by_csat AS (PARTITION BY MONTH(opened_at), csat ORDER BY csat, id)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN actor = 'agent' THEN 1 END) AS event_actor_count,
        COUNT(*) FILTER (WHERE event_type <> 'replied') AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM windowed s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM with_counts s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(priority) OVER w_prev3_closed_at_to_date_csat) AS distinct_priority_in_prev3_closed_at_to_date_csa,
        ROW_NUMBER() OVER w_csat_rank_by_csat_rank AS csat_rank_seq,
        COUNT(*) OVER (PARTITION BY TO_DATE(prev3_closed_at), csat) AS rows_in_prev3_closed_at_to_date_csat,
        MAX(csat_rank) OVER (ORDER BY event_actor_count DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_rank_max_2_prior,
        ROUND(requester_id_bucket / NULLIF(SUM(requester_id_bucket) OVER (PARTITION BY TO_DATE(prev3_closed_at), csat), 0), 4) AS requester_id_bucket_share_of_prev3_closed_at_to_
    FROM profiled
    WINDOW w_prev3_closed_at_to_date_csat AS (PARTITION BY TO_DATE(prev3_closed_at), csat), w_csat_rank_by_csat_rank AS (PARTITION BY csat_rank ORDER BY csat_rank DESC, id)
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    csat,
    tags,
    priority_ref,
    priority_opened_at_month_sum_csat,
    distinct_agent_id_in_priority_opened_at_month,
    prev3_closed_at,
    rows_in_priority_opened_at_month,
    event_actor_count,
    event_event_type_count,
    event_count,
    csat_rank_seq,
    rows_in_prev3_closed_at_to_date_csat,
    csat_rank_max_2_prior,
    requester_id_bucket_share_of_prev3_closed_at_to_
FROM with_windows
ORDER BY id
