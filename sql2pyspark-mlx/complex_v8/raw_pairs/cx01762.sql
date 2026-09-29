WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'replied'
      AND ticket_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        NTILE(5) OVER w_event_at_month_by_event_at AS event_at_quintile,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        NTILE(3) OVER w_event_at_month_by_event_at AS event_at_tercile,
        LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS prev_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at
    FROM filtered
    WINDOW w_event_at_month_by_event_at AS (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    event_at_tercile,
    ticket_id,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(event_at_quintile) / NULLIF(SUM(event_at_tercile), 0), 3) AS event_at_quintile_to_event_at_tercile_ratio
FROM metrics
GROUP BY GROUPING SETS ((event_at_tercile, ticket_id), (event_at_tercile))
ORDER BY event_at_tercile NULLS LAST, ticket_id NULLS LAST
