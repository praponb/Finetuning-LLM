WITH filtered AS (
    -- Filter tickets
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
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year
    FROM tickets
    WHERE priority IN ('P2', 'P3', 'P4')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count
    FROM filtered s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        csat - event_count AS csat_minus_event_count,
        GREATEST(requester_id, event_count) AS greatest_requester_id_event_count,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at
    FROM with_stats
    WHERE priority = 'P4'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        opened_at_year,
        event_count,
        csat_minus_event_count,
        greatest_requester_id_event_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel ORDER BY greatest_requester_id_event_count DESC), 3) AS greatest_requester_id_event_count_percent_rank,
        ROUND((UNIX_TIMESTAMP(last_event_at) - UNIX_TIMESTAMP(LAG(last_event_at) OVER (PARTITION BY channel ORDER BY greatest_requester_id_event_count DESC, id))) / 60, 1) AS minutes_since_prev_last_event_at,
        ROUND(event_count / NULLIF(SUM(event_count) OVER w_priority_last_event_at_year, 0), 4) AS event_count_share_of_priority_last_event_at_year,
        ROUND(AVG(csat_minus_event_count) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at_year), channel ORDER BY csat_minus_event_count ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS csat_minus_event_count_avg_2_prior
    FROM scored
    WINDOW w_priority_last_event_at_year AS (PARTITION BY priority, YEAR(last_event_at))
)
-- Final projection
SELECT
    id,
    opened_at,
    channel,
    opened_at_year,
    csat_minus_event_count,
    greatest_requester_id_event_count,
    greatest_requester_id_event_count_percent_rank,
    minutes_since_prev_last_event_at,
    event_count_share_of_priority_last_event_at_year,
    csat_minus_event_count_avg_2_prior,
    ROUND(event_count / NULLIF(csat, 0), 3) AS event_count_per_csat,
    CASE WHEN channel IN ('web', 'chat') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
FROM metrics
ORDER BY channel_group DESC, csat_minus_event_count_avg_2_prior DESC
