WITH with_dims AS (
    -- Attach sessions attributes
    SELECT e.*, d.browser
    FROM web_events e
    INNER JOIN sessions d
        ON e.session_id = d.id
    WHERE d.customer_id IN (16, 23, 25, 35)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY TO_DATE(event_time), page ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        LEAD(duration_sec, 1) OVER (PARTITION BY DATE_TRUNC('WEEK', event_time), event_type ORDER BY event_time, id) AS next_duration_sec,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS event_seq
    FROM with_dims
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        duration_sec,
        browser,
        next_duration_sec,
        event_seq,
        SUM(duration_sec_bucket) OVER (PARTITION BY page, session_id ORDER BY duration_sec_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_bucket,
        MAX(event_seq) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS event_seq_max_7d
    FROM with_windows
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY duration_sec NULLS LAST, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    ROUND(SUM(running_duration_sec_bucket) / NULLIF(COUNT(CASE WHEN browser = 'Chrome' THEN 1 END), 0), 2) AS running_duration_sec_bucket_per_chrome,
    COUNT_IF(browser = 'Safari') AS safari_events,
    COUNT_IF(browser = 'Edge') AS edge_events,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM deduped
GROUP BY CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY safari_events DESC NULLS FIRST
