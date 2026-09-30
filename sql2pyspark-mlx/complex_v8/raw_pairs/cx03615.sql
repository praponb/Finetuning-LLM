WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-01-31'
      AND ticket_id <= 7
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_year,
        event_type_or_unknown,
        COUNT(*) OVER w_actor AS rows_in_actor,
        ROUND(event_at_year / NULLIF(SUM(event_at_year) OVER (PARTITION BY actor), 0), 4) AS event_at_year_share_of_actor,
        LEAD(event_at, 3) OVER w_event_type_ticket_id_by_even AS next3_event_at,
        ROUND(event_at_year - AVG(event_at_year) OVER w_actor, 2) AS event_at_year_vs_actor_avg,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM prepared_events
    WINDOW w_actor AS (PARTITION BY actor), w_event_type_ticket_id_by_even AS (PARTITION BY event_type, ticket_id ORDER BY event_at_year DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_at_year,
        rows_in_actor,
        event_at_year_share_of_actor,
        next3_event_at,
        last_event_at,
        ROUND((UNIX_TIMESTAMP(next3_event_at) - UNIX_TIMESTAMP(event_at)) / 60, 1) AS minutes_event_at_to_next3_event_at,
        HOUR(last_event_at) AS last_event_at_hour
    FROM ranked
    WHERE event_at_year_share_of_actor < 0.8
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at_year,
        next3_event_at,
        last_event_at,
        ROUND(AVG(event_at_year_share_of_actor) OVER (PARTITION BY last_event_at_hour, event_at_year ORDER BY last_event_at_hour ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS event_at_year_share_of_actor_avg_2_prior,
        SUM(event_at_year_share_of_actor) OVER w_event_at_year AS event_at_year_sum_event_at_year_share_of_actor,
        NTILE(5) OVER (PARTITION BY last_event_at_hour, event_at_year ORDER BY last_event_at_hour ASC, id) AS last_event_at_hour_quintile,
        MIN(rows_in_actor) OVER (PARTITION BY YEAR(event_at)) AS event_at_year_min_rows_in_actor,
        last_event_at_hour - LAG(last_event_at_hour) OVER (PARTITION BY last_event_at_hour, event_at_year ORDER BY last_event_at_hour ASC, id) AS last_event_at_hour_change
    FROM derived
    WINDOW w_event_at_year AS (PARTITION BY YEAR(event_at))
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', last_event_at) AS days_since_last_event_at,
        COALESCE(event_at_year, 0) AS event_at_year_filled,
        ABS(event_at_year - 2025) AS event_at_year_distance
    FROM metrics
    WHERE last_event_at_hour_quintile < 6
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(event_at_year_distance, event_at_year_filled) AS least_event_at_year_distance_event_at_year_fille,
        ABS(event_at_year_distance - 1114) AS event_at_year_distance_distance
    FROM derived_2
    WHERE MONTH(last_event_at) IN (2, 3)
)
-- Final projection
SELECT
    id,
    event_at_year_share_of_actor_avg_2_prior,
    last_event_at_hour_quintile,
    event_at_year_min_rows_in_actor,
    days_since_last_event_at,
    event_at_year_filled,
    event_at_year_distance_distance
FROM tagged
ORDER BY last_event_at_hour_quintile DESC, event_at_year_min_rows_in_actor DESC
