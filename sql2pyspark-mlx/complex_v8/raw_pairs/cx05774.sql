WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        INITCAP(TRIM(source)) AS source_clean,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE ms_played BETWEEN 72464 AND 230798
),
joined AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.country AS listener_country,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan
    FROM base_plays s
    JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source_clean,
        listener_country,
        listener_plan,
        ROUND(AVG(ms_played_bucket) OVER (PARTITION BY YEAR(played_at) ORDER BY listener_signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_bucket_avg_3,
        ROUND(ms_played_bucket - AVG(ms_played_bucket) OVER w_device_track_id, 2) AS ms_played_bucket_vs_device_track_id_avg,
        DENSE_RANK() OVER (PARTITION BY MONTH(listener_signup_date), listener_country ORDER BY ms_played_bucket, id) AS ms_played_bucket_dense_rank,
        MAX(ms_played_bucket) OVER (PARTITION BY listener_id ORDER BY ms_played_bucket NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_bucket_max_7,
        ROW_NUMBER() OVER (PARTITION BY MONTH(listener_signup_date), listener_country ORDER BY ms_played_bucket, id) AS ms_played_bucket_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(played_at) ORDER BY listener_signup_date, id), 3) AS listener_signup_date_percent_rank,
        RANK() OVER (PARTITION BY YEAR(played_at) ORDER BY listener_signup_date) AS listener_signup_date_rank,
        SUM(ms_played_bucket) OVER (PARTITION BY listener_id ORDER BY ms_played_bucket NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_bucket_sum_6_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY ms_played_bucket NULLS LAST), 3) AS ms_played_bucket_cume_dist,
        MIN(ms_played_bucket) OVER (PARTITION BY device, track_id) AS device_track_id_min_ms_played_bucket,
        LAG(played_at, 3) OVER (PARTITION BY MONTH(listener_signup_date), listener_country ORDER BY ms_played_bucket, id) AS prev3_played_at,
        RANK() OVER (PARTITION BY MONTH(listener_signup_date), listener_country ORDER BY ms_played_bucket) AS ms_played_bucket_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(listener_signup_date), listener_country ORDER BY ms_played_bucket) AS ms_played_bucket_dense_rank_2,
        ROUND(ms_played_bucket / NULLIF(SUM(ms_played_bucket) OVER (PARTITION BY device, track_id), 0), 4) AS ms_played_bucket_share_of_device_track_id
    FROM joined
    WINDOW w_device_track_id AS (PARTITION BY device, track_id)
),
stats AS (
    -- Summarise per group
    SELECT
        listener_plan,
        PERCENTILE_APPROX(ms_played_bucket_max_7, 0.5) AS median_ms_played_bucket_max_7,
        MIN(ms_played_bucket_dense_rank) AS min_ms_played_bucket_dense_rank,
        COUNT(DISTINCT listener_plan) AS distinct_listener_plans,
        SUM(CASE WHEN listener_country <> 'JP' THEN device_track_id_min_ms_played_bucket ELSE 0 END) AS device_track_id_min_ms_played_bucket_listener_co,
        ARRAY_SORT(COLLECT_SET(listener_country)) AS listener_country_list
    FROM windowed
    GROUP BY listener_plan
)
-- Final projection
SELECT
    listener_plan,
    median_ms_played_bucket_max_7,
    min_ms_played_bucket_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(listener_plan), ' ', '_')) AS listener_plan_slug
FROM stats
ORDER BY listener_plan, median_ms_played_bucket_max_7 DESC
