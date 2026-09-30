WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(duration_sec) OVER w_hist AS page_avg_duration_sec,
        NTILE(4) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY duration_sec DESC, id ASC) AS duration_sec_bucket,
        COUNT(duration_sec) OVER w_hist_2 AS duration_sec_count_90d,
        MIN(duration_sec) OVER w_hist AS page_min_duration_sec,
        MAX(duration_sec) OVER w_grp AS event_time_year_max_duration_sec
    FROM web_events
    WINDOW w_hist AS (PARTITION BY page), w_hist_2 AS (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY YEAR(event_time), event_type)
)
-- Multi-level aggregation
SELECT
    event_type,
    MIN(event_time) AS first_seen,
    MIN(event_time) AS first_event_time,
    ROUND(AVG(duration_sec_count_90d), 2) AS avg_duration_sec_count_90d
FROM enriched
GROUP BY 1
HAVING avg_duration_sec_count_90d >= 0
ORDER BY avg_duration_sec_count_90d ASC
