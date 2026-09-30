WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        TO_DATE(started_at) AS started_at_date,
        LOWER(device) AS device_lower,
        COALESCE(country, 'N/A') AS country_clean
    FROM sessions
    WHERE started_at > DATE '2023-02-23'
      AND browser IS NOT NULL
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag_2
    FROM sessions_sessions
),
sessions_sessions_2 AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        started_at,
        started_at_date,
        device_lower,
        country_clean,
        SUM(new_group_flag_2) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.device,
        src.started_at_date,
        src.country_clean,
        src.island_id,
        d.age,
        d.country
    FROM sessions_sessions_2 src
    JOIN customers d
        ON src.customer_id = d.id
)
-- Multi-level aggregation
SELECT
    country,
    MIN(age) AS min_age,
    SUM(island_id) AS total_island_id,
    COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count,
    ROUND(AVG(age), 2) AS avg_age
FROM joined
GROUP BY country
