WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 8 AND 12
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(event_at_hour) OVER w_event_at_hour AS event_at_hour_sum_event_at_hour,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        ROUND(event_at_hour / NULLIF(SUM(event_at_hour) OVER (PARTITION BY event_at_hour), 0), 4) AS event_at_hour_share_of_event_at_hour,
        COUNT(*) OVER (PARTITION BY event_at_hour) AS rows_in_event_at_hour,
        ROUND(AVG(event_at_hour) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS event_at_hour_avg_1_prior
    FROM recent_events
    WINDOW w_event_at_hour AS (PARTITION BY event_at_hour), w_all_by_event_at_ties AS (ORDER BY event_at DESC)
)
-- Subtotals with GROUPING SETS
SELECT
    event_at_hour,
    ticket_id,
    ROUND(100 * COUNT(CASE WHEN event_type NOT IN ('replied', 'assigned') THEN 1 END) / COUNT(*), 1) AS pct_event_type_not_in_replied_as,
    MAX(first_event_at) AS last_first_event_at
FROM with_trends
GROUP BY GROUPING SETS ((event_at_hour, ticket_id), (event_at_hour), (ticket_id), ())
