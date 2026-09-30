WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE track_id IS NOT NULL
      AND NOT is_skipped
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        l.signup_date AS listener_signup_date,
        t.moods AS track_moods,
        t.bpm AS track_bpm
    FROM filtered_plays s
    JOIN listeners l ON l.id = s.listener_id AND l.plan NOT IN ('student', 'free')
    LEFT JOIN tracks t ON t.id = s.track_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        ms_played,
        device,
        listener_signup_date,
        track_moods,
        track_bpm,
        FIRST_VALUE(listener_signup_date) OVER (PARTITION BY is_skipped ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_listener_signup_date,
        COUNT(*) OVER (ORDER BY listener_signup_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(4) OVER w_track_id_is_skipped_by_playe AS played_at_quartile,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY YEAR(listener_signup_date), is_skipped ORDER BY track_bpm ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id, is_skipped ORDER BY played_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        ROW_NUMBER() OVER (PARTITION BY YEAR(listener_signup_date), is_skipped ORDER BY track_bpm ASC NULLS LAST, id) AS seq_no,
        SUM(track_bpm) OVER (PARTITION BY YEAR(played_at), track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_track_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, is_skipped ORDER BY played_at, id), 3) AS played_at_percent_rank,
        MIN(ms_played) OVER w_is_skipped AS is_skipped_min_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY MONTH(listener_signup_date)), 2) AS ms_played_vs_listener_signup_date_month_avg,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY MONTH(listener_signup_date)), 0), 4) AS ms_played_share_of_listener_signup_date_month,
        SUM(ms_played) OVER (PARTITION BY track_id, is_skipped ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_sum_3,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY MONTH(listener_signup_date))) AS distinct_listener_id_in_listener_signup_date_mon
    FROM with_parent
    WINDOW w_track_id_is_skipped_by_playe AS (PARTITION BY track_id, is_skipped ORDER BY played_at, id), w_is_skipped AS (PARTITION BY is_skipped)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE seq_no <= 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        device,
        track_moods,
        first_listener_signup_date,
        running_count,
        played_at_quartile,
        minutes_since_prev_played_at,
        rows_in_is_skipped,
        ms_played_avg_2,
        seq_no,
        is_skipped_min_ms_played,
        ms_played_vs_listener_signup_date_month_avg,
        distinct_listener_id_in_listener_signup_date_mon,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY device ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        LAG(listener_signup_date) OVER (PARTITION BY MONTH(listener_signup_date) ORDER BY listener_signup_date ASC, id) AS prev_listener_signup_date,
        NTH_VALUE(running_count, 2) OVER (ORDER BY distinct_listener_id_in_listener_signup_date_mon DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_count
    FROM first_rows
),
per_group AS (
    -- Group-level totals
    SELECT
        TO_DATE(played_at) AS played_at_day,
        SUM(CASE WHEN device = 'phone' THEN 1 ELSE 0 END) AS n_device_phone,
        COUNT(*) AS row_count,
        SUM(played_at_quartile) AS total_played_at_quartile,
        PERCENTILE_APPROX(played_at_quartile, 0.5) AS median_played_at_quartile,
        SUM(minutes_since_prev_played_at) AS total_minutes_since_prev_played_at
    FROM ranked
    GROUP BY TO_DATE(played_at)
)
-- Final projection
SELECT
    played_at_day,
    n_device_phone,
    row_count,
    total_played_at_quartile,
    median_played_at_quartile,
    total_played_at_quartile + n_device_phone AS total_played_at_quartile_plus_n_device_phone
FROM per_group
WHERE total_played_at_quartile <= 45
ORDER BY median_played_at_quartile
