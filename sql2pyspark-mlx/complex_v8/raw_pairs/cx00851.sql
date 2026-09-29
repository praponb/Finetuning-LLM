WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LOWER(TRIM(event_type)) AS event_type_clean,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_type = 'reopened'
),
actor_stats AS (
    -- Avg days_since_event_at per actor
    SELECT actor, AVG(days_since_event_at) AS avg_days_since_event_at
    FROM valid_events
    GROUP BY actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_event_at,
        ROUND(s.days_since_event_at - g.avg_days_since_event_at, 2) AS days_since_event_at_minus_avg
    FROM valid_events s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.days_since_event_at > g.avg_days_since_event_at
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        avg_days_since_event_at,
        days_since_event_at_minus_avg,
        LAST_VALUE(avg_days_since_event_at) OVER (PARTITION BY event_type, actor ORDER BY days_since_event_at_minus_avg ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_avg_days_since_event_at,
        ROUND(avg_days_since_event_at / NULLIF(SUM(avg_days_since_event_at) OVER (PARTITION BY ticket_id, actor), 0), 4) AS avg_days_since_event_at_share_of_ticket_id_actor,
        SUM(days_since_event_at) OVER (PARTITION BY event_type, actor ORDER BY days_since_event_at_minus_avg ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_event_at_sum_1_prior
    FROM outliers
),
derived AS (
    -- Add calculated columns
    SELECT *, HOUR(event_at) AS event_at_hour
    FROM with_trends
    WHERE last_avg_days_since_event_at < 455.0
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor,
    avg_days_since_event_at,
    days_since_event_at_minus_avg,
    last_avg_days_since_event_at,
    avg_days_since_event_at_share_of_ticket_id_actor,
    days_since_event_at_sum_1_prior,
    event_at_hour
FROM derived
WHERE avg_days_since_event_at_share_of_ticket_id_actor > 0.2
ORDER BY days_since_event_at_minus_avg DESC, id
LIMIT 10
