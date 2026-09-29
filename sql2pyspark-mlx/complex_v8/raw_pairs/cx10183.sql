WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE kwh <= 81.3
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM base_readings
    WHERE kwh < (SELECT MAX(kwh) FROM base_readings)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        MAX(kwh) OVER w_read_at_to_date AS read_at_to_date_max_kwh,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        DENSE_RANK() OVER w_read_at_month_site_id_by_rea_2 AS read_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at, id))) / 60, 1) AS minutes_since_prev_read_at
    FROM screened
    WINDOW w_read_at_to_date AS (PARTITION BY TO_DATE(read_at)), w_read_at_month_site_id_by_rea_2 AS (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id ORDER BY read_at, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(minutes_since_prev_read_at - AVG(minutes_since_prev_read_at) OVER (PARTITION BY MONTH(read_at), read_at_dense_rank), 2) AS minutes_since_prev_read_at_vs_read_at_month_read,
        LAG(read_at_dense_rank, 2) OVER (ORDER BY read_at ASC, id) AS prev2_read_at_dense_rank,
        LAST_VALUE(minutes_since_prev_read_at) IGNORE NULLS OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_minutes_since_prev_read_at
    FROM windowed
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    read_at_to_date_max_kwh,
    first_known_kwh,
    minutes_since_prev_read_at_vs_read_at_month_read,
    DATE_TRUNC('WEEK', read_at) AS read_at_week,
    ABS(prev2_read_at_dense_rank - 6) AS prev2_read_at_dense_rank_distance
FROM ranked
ORDER BY read_at_to_date_max_kwh DESC NULLS FIRST, id
