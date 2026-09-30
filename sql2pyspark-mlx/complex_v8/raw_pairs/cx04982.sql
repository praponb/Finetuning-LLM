WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        MONTH(event_at) AS event_at_month,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d
    FROM ticket_events
    WHERE actor = 'system'
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(actor, '#', CAST(event_at_month AS STRING)) AS actor_ref,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        DATE_TRUNC('MONTH', event_at_plus_1d) AS event_at_plus_1d_month
    FROM filtered_events
    WHERE event_at < TIMESTAMP '2025-03-02 10:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_month,
        event_at_plus_1d,
        actor_ref,
        days_since_event_at,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at) AS event_at_rank,
        ROUND((UNIX_TIMESTAMP(event_at_plus_1d) - UNIX_TIMESTAMP(LAG(event_at_plus_1d) OVER (PARTITION BY event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_plus_1d
    FROM scored
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_at_month,
    event_at_plus_1d,
    days_since_event_at,
    event_at_rank,
    minutes_since_prev_event_at_plus_1d,
    CASE WHEN event_at_rank >= 8 THEN 'HIGH' WHEN event_at_rank >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS event_at_rank_tier
FROM windowed
