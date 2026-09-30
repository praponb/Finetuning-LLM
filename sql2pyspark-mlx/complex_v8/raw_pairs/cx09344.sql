WITH filtered AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-24 01:00:00'
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(csat) OVER (PARTITION BY event_type ORDER BY closed_at ASC NULLS LAST, id, event_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS csat_avg_4,
        SUM(csat) OVER (PARTITION BY event_type ORDER BY closed_at ASC NULLS LAST, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        MIN(csat) OVER (PARTITION BY event_type ORDER BY closed_at ASC NULLS LAST, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far
    FROM exploded_detail_events
)
-- Final projection
SELECT
    id,
    csat,
    event_id,
    event_at,
    event_type,
    running_csat,
    min_csat_so_far,
    ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
FROM metrics
