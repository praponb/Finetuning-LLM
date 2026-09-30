WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.tags,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM filtered_tickets s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) FILTER (WHERE channel <> 'phone') AS ticket_channel_count,
        COUNT(*) AS ticket_count,
        MAX(closed_at) AS last_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.last_ticket_closed_at
    FROM exploded_detail s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.event_id,
        s.event_at,
        s.event_type,
        s.ticket_channel_count,
        s.last_ticket_closed_at,
        t.id AS event_id_2,
        t.event_type AS event_event_type,
        t.actor AS event_actor_2
    FROM summarized_rows s
    JOIN ticket_events t ON t.ticket_id = s.id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(ticket_channel_count) OVER (PARTITION BY event_event_type ORDER BY opened_at ASC NULLS LAST, id, event_id, event_id_2 ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ticket_channel_count_sum_2_prior,
        NTILE(5) OVER (PARTITION BY event_event_type ORDER BY opened_at ASC NULLS LAST, id, event_id, event_id_2) AS opened_at_quintile,
        ROW_NUMBER() OVER (PARTITION BY event_event_type ORDER BY opened_at ASC NULLS LAST, id, event_id, event_id_2) AS seq_no
    FROM detail_rows_events
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE seq_no = 1
)
SELECT
    event_type,
    event_id_2,
    event_actor_2,
    ROUND(SUM(seq_no) / NULLIF(SUM(ticket_channel_count_sum_2_prior), 0), 3) AS seq_no_to_ticket_channel_count_sum_2_prior_ratio
FROM top_per_group
GROUP BY GROUPING SETS ((event_type, event_id_2, event_actor_2), (event_type))
ORDER BY event_type NULLS LAST, event_id_2 NULLS LAST, event_actor_2 NULLS LAST
