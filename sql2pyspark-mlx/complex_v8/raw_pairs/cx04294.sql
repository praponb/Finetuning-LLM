WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-08-26' AND DATE '2025-08-02'
      AND duration_sec BETWEEN 184 AND 353
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(release_date_week) OVER (ORDER BY bpm ASC, id) AS prev_release_date_week,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY MONTH(release_date_week) ORDER BY release_date_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        LAG(bpm) OVER w_is_explicit_by_bpm AS prev_bpm,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_is_explicit_artist_id, 0), 4) AS duration_sec_share_of_is_explicit_artist_id,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_is_explicit_artist_id, 0), 4) AS bpm_share_of_is_explicit_artist_id,
        LEAD(duration_sec, 3, 0) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id) AS next3_duration_sec,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_2,
        ROUND((UNIX_TIMESTAMP(release_date_week) - UNIX_TIMESTAMP(LAG(release_date_week) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id))) / 60, 1) AS minutes_since_prev_release_date_week,
        LAG(duration_sec, 2, 0) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date_week) ORDER BY release_date_week ASC, id) AS prev2_duration_sec,
        SUM(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS bpm_sum_4
    FROM prepared_tracks
    WINDOW w_is_explicit_by_bpm AS (PARTITION BY is_explicit ORDER BY bpm DESC, id), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(prev_release_date_week) - UNIX_TIMESTAMP(LAG(prev_release_date_week) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date_week DESC, id))) / 60, 1) AS minutes_since_prev_prev_release_date_week,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit) AS distinct_artist_id_in_is_explicit,
        LAST_VALUE(second_duration_sec_2) OVER (PARTITION BY YEAR(release_date_week), artist_id ORDER BY next3_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_second_duration_sec_2,
        ROUND(AVG(first_duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_release_date_week)), 2) AS prev_release_date_week_month_avg_first_duration_,
        LAG(second_duration_sec, 1, 0) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date_week DESC, id) AS prev_second_duration_sec,
        SIZE(COLLECT_SET(artist_id) OVER w_prev_release_date_week_month) AS distinct_artist_id_in_prev_release_date_week_mon,
        ROUND(AVG(prev2_duration_sec) OVER (PARTITION BY YEAR(release_date_week), artist_id ORDER BY next3_duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS prev2_duration_sec_avg_2_prior
    FROM ranked
    WINDOW w_is_explicit AS (PARTITION BY is_explicit), w_prev_release_date_week_month AS (PARTITION BY DATE_TRUNC('MONTH', prev_release_date_week))
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    bpm,
    moods,
    prev_release_date_week,
    first_duration_sec,
    prev_bpm,
    duration_sec_share_of_is_explicit_artist_id,
    bpm_share_of_is_explicit_artist_id,
    second_duration_sec_2,
    minutes_since_prev_release_date_week,
    prev2_duration_sec,
    bpm_sum_4,
    last_second_duration_sec_2,
    prev_release_date_week_month_avg_first_duration_,
    prev_second_duration_sec,
    distinct_artist_id_in_prev_release_date_week_mon,
    LEAST(prev_second_duration_sec, second_duration_sec_2) AS least_prev_second_duration_sec_second_duration_s
FROM with_windows
