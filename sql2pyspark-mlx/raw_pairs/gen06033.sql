WITH cleaned AS (
    SELECT
        *,
        ROUND(duration_sec, 0) AS duration_sec_rounded,
        COALESCE(page, 'NONE') AS page_clean
    FROM web_events
    WHERE event_time > DATE '2023-08-06'
),
enriched AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        page_clean,
        AVG(duration_sec) OVER (PARTITION BY event_type, session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY event_type, session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type, session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_duration_sec,
        LAG(duration_sec_rounded, 1, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', event_time), page ORDER BY duration_sec, id) AS prev_duration_sec_rounded
    FROM cleaned
),
scored AS (
    SELECT
        *,
        LAG(prev_duration_sec_rounded, 1, 0) OVER w_grp AS prev_prev_duration_sec_rounded,
        NTILE(4) OVER w_grp AS prev_duration_sec_rounded_bucket,
        COUNT(running_sum_duration_sec) OVER (PARTITION BY event_type, page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS running_sum_duration_sec_count_30d,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY prev_duration_sec_rounded, id) AS prev_duration_sec_rounded_bucket_2,
        NTILE(5) OVER (PARTITION BY event_type, page ORDER BY running_avg_duration_sec, id) AS running_avg_duration_sec_bucket
    FROM enriched
    WINDOW w_grp AS (ORDER BY prev_duration_sec_rounded, id)
),
remaining_keys AS (
    SELECT id, event_type
    FROM scored
    EXCEPT ALL
    SELECT id, event_type
    FROM web_events
    WHERE event_type = 'page_view'
),
reconciled AS (
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.event_type = k.event_type
),
with_windows AS (
    SELECT
        id,
        event_type,
        event_time,
        running_sum_duration_sec,
        prev_duration_sec_rounded,
        prev_prev_duration_sec_rounded,
        prev_duration_sec_rounded_bucket,
        running_sum_duration_sec_count_30d,
        prev_duration_sec_rounded_bucket_2,
        ROUND(running_sum_duration_sec_count_30d / SUM(running_sum_duration_sec_count_30d) OVER (PARTITION BY page), 4) AS running_sum_duration_sec_count_30d_share,
        RANK() OVER (PARTITION BY event_type, page ORDER BY running_sum_duration_sec_count_30d DESC NULLS LAST) AS running_sum_duration_sec_count_30d_rank
    FROM reconciled
),
valid AS (
    SELECT *, CAST(prev_duration_sec_rounded_bucket_2 AS DOUBLE) AS prev_duration_sec_rounded_bucket_2_str
    FROM with_windows
    WHERE prev_duration_sec_rounded BETWEEN 54 AND 58
),
metrics AS (
    SELECT
        event_time,
        prev_duration_sec_rounded,
        prev_prev_duration_sec_rounded,
        prev_duration_sec_rounded_bucket,
        running_sum_duration_sec_count_30d,
        prev_duration_sec_rounded_bucket_2,
        running_sum_duration_sec_count_30d_share,
        running_sum_duration_sec_count_30d_rank,
        prev_duration_sec_rounded_bucket_2_str,
        CUME_DIST() OVER w_ordered AS prev_duration_sec_rounded_cume_dist
    FROM valid
    WINDOW w_ordered AS (ORDER BY prev_duration_sec_rounded DESC)
),
daily AS (
    SELECT
        CASE WHEN prev_prev_duration_sec_rounded >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_prev_duration_sec_rounded_tier,
        CASE WHEN prev_duration_sec_rounded_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS prev_duration_sec_rounded_bucket_tier,
        MAX(running_sum_duration_sec_count_30d) AS max_running_sum_duration_sec_count_30d,
        APPROX_PERCENTILE(prev_prev_duration_sec_rounded, 0.95) AS prev_prev_duration_sec_rounded_p95,
        MAX(event_time) AS last_seen,
        MAX(event_time) AS last_seen_2,
        COUNT(*) AS row_count
    FROM metrics
    GROUP BY GROUPING SETS ((CASE WHEN prev_prev_duration_sec_rounded >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_duration_sec_rounded_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN prev_prev_duration_sec_rounded >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
)
SELECT
    prev_prev_duration_sec_rounded_tier,
    prev_duration_sec_rounded_bucket_tier,
    max_running_sum_duration_sec_count_30d,
    prev_prev_duration_sec_rounded_p95,
    last_seen,
    last_seen_2,
    row_count
FROM daily
