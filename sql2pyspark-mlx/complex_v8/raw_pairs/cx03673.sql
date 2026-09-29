WITH joined_ranges AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        ticket_id,
        CASE WHEN hours_since_open >= 43.2 THEN 'A' WHEN hours_since_open >= 28.8 THEN 'B' ELSE 'C' END AS hours_since_open_tier,
        event_type,
        MIN(event_at) AS first_event_at,
        ROUND(SUM(hours_since_open), 2) AS total_hours_since_open,
        ROUND(AVG(hours_since_open), 2) AS avg_hours_since_open,
        COUNT(*) FILTER (WHERE priority NOT IN ('P3', 'P2')) AS n_priority_not_in_p3_p2,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        SUM(hours_since_open) AS total_hours_since_open_2
    FROM joined_ranges
    GROUP BY ticket_id, CASE WHEN hours_since_open >= 43.2 THEN 'A' WHEN hours_since_open >= 28.8 THEN 'B' ELSE 'C' END, event_type
),
annotated AS (
    -- Derive helper columns
    SELECT
        ticket_id,
        hours_since_open_tier,
        event_type,
        first_event_at,
        total_hours_since_open,
        n_priority_not_in_p3_p2,
        distinct_agent_ids,
        total_hours_since_open_2,
        DAYOFWEEK(first_event_at) AS first_event_at_dayofweek,
        COALESCE(total_hours_since_open, 0.0) AS total_hours_since_open_filled
    FROM rollup_base
)
SELECT
    ticket_id,
    hours_since_open_tier,
    event_type,
    first_event_at,
    total_hours_since_open,
    n_priority_not_in_p3_p2,
    total_hours_since_open_2,
    first_event_at_dayofweek,
    total_hours_since_open_filled,
    ROUND(total_hours_since_open * 1.5, 2) AS total_hours_since_open_adj,
    CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
FROM annotated
WHERE hours_since_open_tier = 'A'
ORDER BY first_event_at DESC, ticket_id, hours_since_open_tier, event_type
LIMIT 20
