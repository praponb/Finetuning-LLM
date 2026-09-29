WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        DATE_TRUNC('DAY', played_at) AS played_at_day
    FROM plays
    WHERE is_skipped = TRUE
      AND track_id IS NOT NULL
),
joined AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.played_at_day,
        l.country AS listener_country,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan
    FROM filtered s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(played_at_day) - UNIX_TIMESTAMP(LAG(played_at_day) OVER (PARTITION BY listener_plan ORDER BY played_at_day, id))) / 60, 1) AS minutes_since_prev_played_at_day,
        ROUND(PERCENT_RANK() OVER w_device_played_at_day_year_by, 3) AS played_at_day_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY played_at_day), 3) AS played_at_day_cume_dist,
        LEAD(played_at, 2) OVER (ORDER BY listener_signup_date, id) AS next2_played_at,
        LEAD(played_at_day) OVER (PARTITION BY device, YEAR(played_at_day) ORDER BY played_at_day DESC, id) AS next_played_at_day,
        FIRST_VALUE(played_at) OVER (PARTITION BY listener_plan ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        DENSE_RANK() OVER (PARTITION BY listener_plan ORDER BY played_at DESC NULLS FIRST, id) AS played_at_dense_rank,
        NTILE(3) OVER (ORDER BY listener_signup_date, id) AS listener_signup_date_tercile,
        LAG(listener_signup_date, 3) OVER (PARTITION BY device, track_id ORDER BY played_at, id) AS prev3_listener_signup_date,
        LEAD(listener_signup_date) OVER (ORDER BY listener_signup_date, id) AS next_listener_signup_date,
        RANK() OVER (PARTITION BY listener_plan ORDER BY played_at_day, id) AS played_at_day_rank,
        LAG(listener_signup_date, 2) OVER (ORDER BY listener_signup_date, id) AS prev2_listener_signup_date,
        LAST_VALUE(listener_signup_date) IGNORE NULLS OVER (PARTITION BY device, YEAR(played_at_day) ORDER BY played_at_day DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_listener_signup_date
    FROM joined
    WINDOW w_device_played_at_day_year_by AS (PARTITION BY device, YEAR(played_at_day) ORDER BY played_at_day DESC)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        played_at_day,
        minutes_since_prev_played_at_day,
        played_at_day_percent_rank,
        played_at_day_cume_dist,
        next2_played_at,
        next_played_at_day,
        first_played_at,
        played_at_dense_rank,
        listener_signup_date_tercile,
        next_listener_signup_date,
        played_at_day_rank,
        prev2_listener_signup_date,
        LAST_DAY(listener_signup_date) AS listener_signup_date_month_end
    FROM with_trends
    WHERE HOUR(first_played_at) BETWEEN 8 AND 10
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        played_at,
        device,
        is_skipped,
        played_at_day,
        minutes_since_prev_played_at_day,
        played_at_day_percent_rank,
        next2_played_at,
        next_played_at_day,
        first_played_at,
        played_at_dense_rank,
        played_at_day_rank,
        listener_signup_date_month_end,
        COUNT(*) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at)) AS rows_in_track_id_played_at_month,
        ROUND(played_at_day_cume_dist / NULLIF(SUM(played_at_day_cume_dist) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at)), 0), 4) AS played_at_day_cume_dist_share_of_track_id_played,
        SUM(listener_signup_date_tercile) OVER (ORDER BY first_played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_listener_signup_date_tercile,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id_played_at_month) AS distinct_listener_id_in_track_id_played_at_month,
        NTH_VALUE(next_listener_signup_date, 2) OVER (ORDER BY prev2_listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_listener_signup_date,
        NTILE(4) OVER (ORDER BY first_played_at, id) AS first_played_at_quartile
    FROM tagged
    WINDOW w_track_id_played_at_month AS (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at))
),
stats AS (
    -- Summarise per group
    SELECT
        played_at_day_rank,
        DATE_FORMAT(listener_signup_date_month_end, 'yyyy-MM') AS listener_signup_date_month_end_ym,
        first_played_at_quartile,
        SUM(first_played_at_quartile) AS total_first_played_at_quartile,
        MIN(played_at) AS first_played_at_2,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list
    FROM windowed
    GROUP BY played_at_day_rank, DATE_FORMAT(listener_signup_date_month_end, 'yyyy-MM'), first_played_at_quartile
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY first_played_at_quartile, played_at_day_rank ORDER BY total_first_played_at_quartile ASC), 3) AS total_first_played_at_quartile_cume_dist,
        ROUND(AVG(first_played_at_quartile) OVER (PARTITION BY first_played_at_quartile, played_at_day_rank ORDER BY total_first_played_at_quartile ASC, played_at_day_rank, listener_signup_date_month_end_ym, first_played_at_quartile ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS first_played_at_quartile_avg_2_prior,
        ROUND(first_played_at_quartile - AVG(first_played_at_quartile) OVER (PARTITION BY first_played_at_quartile), 2) AS first_played_at_quartile_vs_first_played_at_quar
    FROM stats
)
-- Final projection
SELECT
    played_at_day_rank,
    listener_signup_date_month_end_ym,
    first_played_at_quartile,
    total_first_played_at_quartile,
    device_list,
    first_played_at_quartile_avg_2_prior,
    first_played_at_quartile_vs_first_played_at_quar,
    CASE WHEN first_played_at_quartile >= 8 THEN 'large' WHEN first_played_at_quartile >= 5 THEN 'medium' ELSE 'small' END AS first_played_at_quartile_tier,
    FLOOR(first_played_at_quartile / 2) * 2 AS first_played_at_quartile_bucket
FROM sequenced
ORDER BY first_played_at_quartile_bucket DESC NULLS FIRST
