WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type <> 'resolved'
),
totals AS (
    -- Group-level totals
    SELECT
        event_type,
        SUM(CASE WHEN actor IS NOT NULL THEN 1 ELSE 0 END) AS n_actor_is_not_null,
        MAX(event_at_month) AS last_event_at_month,
        COUNT(*) AS cnt
    FROM cleaned
    WHERE event_at_quarter < TIMESTAMP '2025-02-13 09:00:00'
    GROUP BY 1
),
tagged AS (
    -- Label and score rows
    SELECT
        event_type,
        n_actor_is_not_null,
        last_event_at_month,
        DAYOFWEEK(last_event_at_month) AS last_event_at_month_dayofweek,
        ABS(cnt - 6) AS cnt_distance,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM totals
)
-- Native SQL UNPIVOT (wide to long)
SELECT event_type, field, amount
FROM (
    SELECT event_type, last_event_at_month_dayofweek, n_actor_is_not_null, cnt_distance
    FROM tagged
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (last_event_at_month_dayofweek AS m_last_event_at_month_dayofweek, n_actor_is_not_null AS m_n_actor_is_not_null, cnt_distance AS cnt_distance_v)
) unp
ORDER BY event_type, field
