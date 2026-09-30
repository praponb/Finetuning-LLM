WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 09:00:00'
      AND actor IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type)) AS distinct_actor_in_event_type,
        NTILE(5) OVER (PARTITION BY ticket_id ORDER BY days_since_event_at, id) AS days_since_event_at_quintile,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY event_type), 2) AS days_since_event_at_vs_event_type_avg,
        NTILE(4) OVER (PARTITION BY MONTH(event_at) ORDER BY days_since_event_at, id) AS days_since_event_at_quartile,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY days_since_event_at) AS days_since_event_at_dense_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY days_since_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM valid
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
),
actor_stats AS (
    -- Avg days_since_event_at_vs_event_type_avg per actor
    SELECT actor, AVG(days_since_event_at_vs_event_type_avg) AS avg_days_since_event_at_vs_event_type_avg
    FROM screened
    GROUP BY actor
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_event_at_vs_event_type_avg,
        ROUND(s.days_since_event_at_vs_event_type_avg - g.avg_days_since_event_at_vs_event_type_avg, 2) AS days_since_event_at_vs_event_type_avg_minus_avg
    FROM screened s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.days_since_event_at_vs_event_type_avg > g.avg_days_since_event_at_vs_event_type_avg
)
SELECT
    distinct_actor_in_event_type,
    days_since_event_at_quintile,
    days_since_event_at_quartile,
    ROUND(AVG(days_since_event_at_vs_event_type_avg_minus_avg), 2) AS avg_days_since_event_at_vs_event_type_avg_minus_,
    MAX(days_since_event_at_vs_event_type_avg_minus_avg) AS max_days_since_event_at_vs_event_type_avg_minus_,
    ROUND(AVG(days_since_event_at), 2) AS avg_days_since_event_at
FROM above_average
GROUP BY GROUPING SETS ((distinct_actor_in_event_type, days_since_event_at_quintile, days_since_event_at_quartile), (distinct_actor_in_event_type), (days_since_event_at_quintile), ())
