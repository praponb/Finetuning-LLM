WITH base AS (
    SELECT
        *,
        CONCAT_WS('-', event_type, page) AS event_type_page_key,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE page IN ('/search', '/home', '/product')
),
with_windows AS (
    SELECT
        *,
        NTILE(3) OVER (ORDER BY duration_sec ASC, id ASC) AS duration_sec_bucket,
        CUME_DIST() OVER (PARTITION BY session_id ORDER BY duration_sec ASC NULLS FIRST) AS duration_sec_cume_dist,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id ASC) AS seq_no,
        LAG(duration_sec, 2) OVER w_part AS prev_duration_sec
    FROM base
    WINDOW w_part AS (PARTITION BY event_type ORDER BY event_time ASC, id ASC)
),
metrics AS (
    SELECT
        *,
        MAX(prev_duration_sec) OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_duration_sec,
        SUM(prev_duration_sec) OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_duration_sec,
        COUNT(prev_duration_sec) OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_prev_duration_sec,
        MIN(prev_duration_sec) OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_prev_duration_sec
    FROM with_windows
),
deduped AS (
    SELECT
        id,
        event_type,
        page,
        seq_no,
        running_max_prev_duration_sec,
        running_sum_prev_duration_sec,
        running_min_prev_duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY seq_no DESC NULLS LAST, id ASC) AS dedup_rank
        FROM metrics
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    SELECT
        id,
        event_type,
        seq_no,
        running_sum_prev_duration_sec,
        running_min_prev_duration_sec,
        MAX(running_min_prev_duration_sec) OVER (PARTITION BY page) AS page_max_running_min_prev_duration_sec,
        AVG(running_min_prev_duration_sec) OVER (PARTITION BY page, event_type ORDER BY running_min_prev_duration_sec ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS running_min_prev_duration_sec_avg_2_prior
    FROM deduped
)
SELECT
    id,
    event_type,
    metric_name,
    val
FROM (
    SELECT
        id,
        event_type,
        running_min_prev_duration_sec,
        running_sum_prev_duration_sec
    FROM scored
) src
UNPIVOT (
    val FOR metric_name IN (running_min_prev_duration_sec AS RUNNING_MIN_PREV_DURATION_SEC, running_sum_prev_duration_sec AS RUNNING_SUM_PREV_DURATION_SEC_VAL)
);
