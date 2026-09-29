WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(event_type) AS event_type_lower,
        SUBSTRING(event_type, 1, 3) AS event_type_prefix,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-05-15 03:00:00'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        page,
        event_time,
        event_type_prefix,
        page_filled,
        LEAD(duration_sec, 1) OVER (ORDER BY duration_sec, id) AS next_duration_sec,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('week', event_time), event_type ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_dense_rank,
        LEAD(duration_sec, 2, 0) OVER (PARTITION BY YEAR(event_time) ORDER BY duration_sec, id) AS next_duration_sec_2,
        MIN(duration_sec) OVER (PARTITION BY session_id) AS session_id_min_duration_sec
    FROM base
    WHERE session_id IN (16, 27)
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.next_duration_sec_2,
        e.session_id_min_duration_sec,
        b.browser,
        b.country,
        b.device
    FROM metrics e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY next_duration_sec_2 DESC NULLS FIRST, id) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(device)) AS device_title,
        COALESCE(device, 'UNKNOWN') AS device_filled
    FROM ranked
    WHERE next_duration_sec_2 >= 467
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_duration_sec_2 >= 462 THEN 'HIGH' ELSE 'LOW' END AS next_duration_sec_2_tier,
    CASE WHEN session_id_min_duration_sec >= 502 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_min_duration_sec_tier,
    browser,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(*) AS num_rows
FROM cleaned
GROUP BY CUBE (CASE WHEN next_duration_sec_2 >= 462 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN session_id_min_duration_sec >= 502 THEN 'PREMIUM' ELSE 'STANDARD' END, browser)
ORDER BY session_id_min_duration_sec_tier DESC NULLS LAST;
