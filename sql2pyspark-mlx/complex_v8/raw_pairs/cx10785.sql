WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE (listener_id >= 13 OR ms_played <= 239955)
),
with_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.ms_played_bucket,
        l.username AS listener_username,
        l.interests AS listener_interests,
        l.plan AS listener_plan,
        t.title AS track_title,
        t.release_date AS track_release_date
    FROM base s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(track_release_date) OVER (PARTITION BY listener_plan ORDER BY ms_played_bucket, id) AS prev_track_release_date,
        NTILE(5) OVER (PARTITION BY is_skipped ORDER BY ms_played_bucket, id) AS ms_played_bucket_quintile,
        LAST_VALUE(ms_played_bucket) OVER (PARTITION BY listener_id, is_skipped ORDER BY track_release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_bucket,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY ms_played_bucket), 3) AS ms_played_bucket_percent_rank,
        SUM(ms_played_bucket) OVER (PARTITION BY listener_plan ORDER BY ms_played_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_bucket_sum_6_prior,
        NTILE(4) OVER (PARTITION BY listener_id, is_skipped ORDER BY track_release_date ASC NULLS LAST, id) AS track_release_date_quartile,
        NTH_VALUE(ms_played_bucket, 2) OVER (PARTITION BY listener_plan ORDER BY ms_played_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_bucket,
        MAX(ms_played_bucket) OVER (PARTITION BY listener_id) AS listener_id_max_ms_played_bucket,
        LAST_VALUE(ms_played_bucket) IGNORE NULLS OVER (PARTITION BY listener_id, is_skipped ORDER BY track_release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_bucket,
        MAX(ms_played_bucket) OVER (PARTITION BY is_skipped ORDER BY track_release_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_bucket_max_4,
        ROUND(ms_played_bucket / NULLIF(SUM(ms_played_bucket) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_bucket_share_of_listener_id,
        SUM(ms_played_bucket) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played_bucket,
        ROUND(AVG(ms_played_bucket) OVER w_device, 2) AS device_avg_ms_played_bucket,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, is_skipped ORDER BY track_release_date ASC NULLS LAST), 3) AS track_release_date_cume_dist,
        NTH_VALUE(track_release_date, 2) OVER (PARTITION BY is_skipped ORDER BY ms_played_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_release_date
    FROM with_details
    WINDOW w_device AS (PARTITION BY device)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(listener_plan), 1, 2) AS listener_plan_prefix,
        GREATEST(ms_played_bucket_max_4, second_ms_played_bucket) AS greatest_ms_played_bucket_max_4_second_ms_played
    FROM windowed
    WHERE track_release_date_cume_dist >= 0.2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        ms_played_bucket,
        listener_username,
        listener_interests,
        listener_plan,
        track_title,
        track_release_date,
        ms_played_bucket_quintile,
        last_ms_played_bucket,
        ms_played_bucket_percent_rank,
        ms_played_bucket_sum_6_prior,
        track_release_date_quartile,
        second_ms_played_bucket,
        last_known_ms_played_bucket,
        ms_played_bucket_max_4,
        ms_played_bucket_share_of_listener_id,
        listener_id_sum_ms_played_bucket,
        track_release_date_cume_dist,
        second_track_release_date,
        listener_plan_prefix,
        greatest_ms_played_bucket_max_4_second_ms_played,
        FIRST_VALUE(ms_played_bucket_sum_6_prior) OVER (PARTITION BY device, is_skipped ORDER BY ms_played_bucket_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played_bucket_sum_6_prior,
        LAST_VALUE(ms_played_bucket_quintile) IGNORE NULLS OVER (PARTITION BY listener_id, ms_played_bucket_quintile ORDER BY second_ms_played_bucket DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_bucket_quintile,
        NTILE(3) OVER w_track_release_date_quartile AS greatest_ms_played_bucket_max_4_second_ms_played_2,
        ms_played_bucket_quintile - LAG(ms_played_bucket_quintile) OVER w_listener_id_ms_played_bucket_2 AS ms_played_bucket_quintile_change,
        FIRST_VALUE(second_ms_played_bucket) IGNORE NULLS OVER (PARTITION BY device, is_skipped ORDER BY ms_played_bucket_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_ms_played_bucket,
        RANK() OVER (PARTITION BY listener_id, ms_played_bucket_quintile ORDER BY second_ms_played_bucket DESC NULLS FIRST) AS second_ms_played_bucket_rank,
        ROUND(AVG(ms_played_bucket_max_4) OVER w_is_skipped_prev_track_releas, 2) AS is_skipped_prev_track_release_date_month_avg_ms_
    FROM tagged
    WINDOW w_track_release_date_quartile AS (PARTITION BY track_release_date_quartile ORDER BY greatest_ms_played_bucket_max_4_second_ms_played ASC, id), w_listener_id_ms_played_bucket_2 AS (PARTITION BY listener_id, ms_played_bucket_quintile ORDER BY second_ms_played_bucket DESC NULLS FIRST, id), w_is_skipped_prev_track_releas AS (PARTITION BY is_skipped, DATE_TRUNC('MONTH', prev_track_release_date))
),
totals AS (
    -- Summarise per group
    SELECT
        ms_played_bucket_quintile,
        greatest_ms_played_bucket_max_4_second_ms_played_2,
        track_release_date_quartile,
        ROUND(STDDEV(greatest_ms_played_bucket_max_4_second_ms_played), 2) AS stddev_greatest_ms_played_bucket_max_4_second_ms,
        SUM(track_release_date_quartile) AS total_track_release_date_quartile,
        MAX(last_ms_played_bucket) AS max_last_ms_played_bucket,
        ROUND(AVG(track_release_date_cume_dist), 2) AS avg_track_release_date_cume_dist,
        MAX(track_release_date) AS last_track_release_date
    FROM with_trends
    GROUP BY 1, 2, 3
    HAVING MAX(last_ms_played_bucket) > 123500
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        ms_played_bucket_quintile,
        greatest_ms_played_bucket_max_4_second_ms_played_2,
        track_release_date_quartile,
        total_track_release_date_quartile,
        max_last_ms_played_bucket,
        avg_track_release_date_cume_dist,
        last_track_release_date,
        MAX(max_last_ms_played_bucket) OVER (PARTITION BY track_release_date_quartile, ms_played_bucket_quintile ORDER BY last_track_release_date DESC, ms_played_bucket_quintile, greatest_ms_played_bucket_max_4_second_ms_played_2, track_release_date_quartile ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS max_last_ms_played_bucket_max_3_prior,
        FIRST_VALUE(last_track_release_date) OVER (PARTITION BY greatest_ms_played_bucket_max_4_second_ms_played_2 ORDER BY max_last_ms_played_bucket DESC, ms_played_bucket_quintile, greatest_ms_played_bucket_max_4_second_ms_played_2, track_release_date_quartile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_track_release_date
    FROM totals
)
-- Final projection
SELECT
    ms_played_bucket_quintile,
    greatest_ms_played_bucket_max_4_second_ms_played_2,
    total_track_release_date_quartile,
    max_last_ms_played_bucket,
    avg_track_release_date_cume_dist,
    last_track_release_date,
    max_last_ms_played_bucket_max_3_prior,
    first_last_track_release_date
FROM with_windows
ORDER BY greatest_ms_played_bucket_max_4_second_ms_played_2 DESC
