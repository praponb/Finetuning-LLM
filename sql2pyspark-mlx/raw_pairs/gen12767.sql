WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC NULLS LAST, id) AS dedup_rank
        FROM web_events
    ) t
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.event_time,
        src.duration_sec,
        p.device,
        p.country,
        p.browser
    FROM ranked src
    LEFT JOIN sessions p
        ON src.session_id = p.id
    WHERE p.customer_id = 35
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT src.*, s.country AS session_country
    FROM joined src
    INNER JOIN sessions s
        ON src.session_id = s.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_avg_5,
        LEAD(duration_sec, 1) OVER (PARTITION BY MONTH(event_time), device ORDER BY event_time, id) AS next_duration_sec,
        LEAD(duration_sec, 2) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS next_duration_sec_2
    FROM joined_web_events
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_duration_sec_2 >= 539 THEN 'HIGH' ELSE 'LOW' END AS next_duration_sec_2_tier,
    COUNT(*) FILTER (WHERE session_country = 'US') AS us_rows,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    SUM(CASE WHEN country = 'JP' THEN next_duration_sec ELSE 0 END) AS jp_next_duration_sec,
    COUNT(*) AS cnt,
    ROUND(AVG(next_duration_sec), 2) AS avg_next_duration_sec
FROM metrics
GROUP BY CASE WHEN next_duration_sec_2 >= 539 THEN 'HIGH' ELSE 'LOW' END
HAVING jp_next_duration_sec > 0
ORDER BY next_duration_sec_2_tier DESC NULLS LAST
