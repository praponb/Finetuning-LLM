WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) / 365.25 AS years_since_event_time,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM web_events
    WHERE event_time >= '2025-09-01'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec >= 476 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    MAX(event_time) AS last_seen,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(duration_sec) AS duration_sec_bit_and
FROM sessionized
GROUP BY CASE WHEN duration_sec >= 476 THEN 'PREMIUM' ELSE 'STANDARD' END
