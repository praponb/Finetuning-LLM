WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        COUNT(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_duration_sec
    FROM web_events
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        last4_count_duration_sec,
        last4_sum_duration_sec,
        COUNT(last4_sum_duration_sec) OVER (PARTITION BY event_type) AS event_type_count_last4_sum_duration_sec,
        LEAD(last4_sum_duration_sec, 1) OVER (ORDER BY last4_min_duration_sec ASC, id ASC) AS next_last4_sum_duration_sec
    FROM scored
    WHERE last4_min_duration_sec <= 280
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(event_type_count_last4_sum_duration_sec) OVER (ORDER BY last4_sum_duration_sec ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_type_count_last4_sum_duration_sec_avg_3_prior,
        DENSE_RANK() OVER (ORDER BY next_last4_sum_duration_sec ASC NULLS LAST) AS next_last4_sum_duration_sec_dense_rank,
        FIRST_VALUE(event_type_count_last4_sum_duration_sec, TRUE) OVER (ORDER BY event_type_count_last4_sum_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_type_count_last4_sum_duration_sec,
        NTILE(3) OVER (ORDER BY last4_sum_duration_sec ASC, id ASC) AS last4_sum_duration_sec_bucket,
        NTILE(5) OVER (ORDER BY last4_sum_duration_sec ASC, id ASC) AS last4_sum_duration_sec_bucket_2
    FROM with_windows
),
with_windows_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last4_sum_duration_sec,
        next_last4_sum_duration_sec,
        last4_sum_duration_sec_bucket,
        SUM(last4_count_duration_sec) OVER (ORDER BY first_known_event_type_count_last4_sum_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last4_count_duration_sec,
        CUME_DIST() OVER (ORDER BY event_type_count_last4_sum_duration_sec_avg_3_prior DESC NULLS FIRST) AS event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist,
        ROW_NUMBER() OVER (ORDER BY event_type_count_last4_sum_duration_sec DESC, id ASC) AS event_seq,
        NTILE(5) OVER (ORDER BY last4_sum_duration_sec DESC, id ASC) AS last4_sum_duration_sec_bucket_3,
        FIRST_VALUE(last4_sum_duration_sec_bucket) IGNORE NULLS OVER (ORDER BY event_type_count_last4_sum_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_sum_duration_sec_bucket
    FROM metrics
),
metrics_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(last4_sum_duration_sec_bucket) IGNORE NULLS OVER (ORDER BY last4_sum_duration_sec_bucket_3 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_sum_duration_sec_bucket_2,
        AVG(event_seq) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_seq_avg_2,
        ROW_NUMBER() OVER (ORDER BY last4_sum_duration_sec ASC, id ASC) AS row_seq
    FROM with_windows_web_events
),
with_windows_web_events_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_seq, 1) OVER (ORDER BY first_known_last4_sum_duration_sec_bucket_2 ASC, id ASC) AS next_event_seq,
        NTILE(10) OVER (ORDER BY last4_sum_duration_sec ASC, id ASC) AS last4_sum_duration_sec_bucket_2,
        AVG(last4_sum_duration_sec_bucket_3) OVER (ORDER BY last4_sum_duration_sec_bucket ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS last4_sum_duration_sec_bucket_3_avg_5,
        ROW_NUMBER() OVER (ORDER BY event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist DESC, id ASC) AS seq_no,
        AVG(last4_sum_duration_sec_bucket_3) OVER (ORDER BY first_known_last4_sum_duration_sec_bucket ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last4_sum_duration_sec_bucket_3_avg_6_prior
    FROM metrics_web_events
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        last4_sum_duration_sec,
        next_last4_sum_duration_sec,
        last4_sum_duration_sec_bucket,
        cumulative_last4_count_duration_sec,
        event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist,
        event_seq,
        last4_sum_duration_sec_bucket_3,
        event_seq_avg_2,
        next_event_seq,
        last4_sum_duration_sec_bucket_2,
        last4_sum_duration_sec_bucket_3_avg_5,
        seq_no,
        last4_sum_duration_sec_bucket_3_avg_6_prior,
        ROUND(next_event_seq, 2) AS next_event_seq_rounded
    FROM with_windows_web_events_2
    WHERE last4_sum_duration_sec_bucket_3_avg_6_prior BETWEEN 4 AND 7
)
-- Grouped metrics
SELECT
    seq_no,
    CASE WHEN event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM filtered
GROUP BY seq_no, event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist_tier
HAVING COUNT(DISTINCT id) > 1
