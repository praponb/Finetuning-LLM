WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(event_at) ORDER BY event_at NULLS LAST), 3) AS event_at_percent_rank_2,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM prepared_events
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    ticket_id,
    GROUPING_ID() AS grouping_level,
    MAX(event_at_percent_rank_2) AS max_event_at_percent_rank_2,
    MIN(event_at_percent_rank_2) AS min_event_at_percent_rank_2,
    COUNT(*) AS row_count
FROM metrics
GROUP BY GROUPING SETS ((actor, event_type, ticket_id), (actor), (event_type), ())
