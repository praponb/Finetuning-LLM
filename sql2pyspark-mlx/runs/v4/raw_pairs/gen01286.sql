WITH valid AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        QUARTER(event_time) event_time_quarter
    FROM web_events
    WHERE page IN ('/cart', '/home', '/checkout')
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time_quarter DESC, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
scored AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        event_time_quarter,
        rn,
        MAX(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_max_7d,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec,
        LAST_VALUE(page) IGNORE NULLS OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_page,
        ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY event_time_quarter, id) event_seq
    FROM deduped
)
SELECT
    CASE WHEN event_seq >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    MAX(running_duration_sec) AS max_running_duration_sec,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    COUNT(*) AS event_count,
    COUNT(DISTINCT page) AS distinct_pages,
    PERCENTILE_APPROX(event_seq, 0.9) AS event_seq_p90,
    COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count
FROM scored
GROUP BY 1;
