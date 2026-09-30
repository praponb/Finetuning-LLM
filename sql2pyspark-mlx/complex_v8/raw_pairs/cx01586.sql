WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
    WHERE release_date < DATE '2025-08-02'
      AND is_explicit = FALSE
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (ORDER BY duration_sec, id) AS duration_sec_tercile,
        ROW_NUMBER() OVER (ORDER BY duration_sec, id) AS rn,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY release_date_dayofweek, is_explicit)) AS distinct_artist_id_in_release_date_dayofweek_is_,
        COUNT(*) OVER (PARTITION BY release_date_dayofweek, is_explicit) AS rows_in_release_date_dayofweek_is_explicit,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id), 3) AS bpm_cume_dist,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_release_date_month_artist_id, 0), 4) AS bpm_share_of_release_date_month_artist_id,
        MAX(release_date_dayofweek) OVER (ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS release_date_dayofweek_max_6_prior,
        NTH_VALUE(release_date_dayofweek, 2) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_dayofweek,
        ROUND(AVG(bpm) OVER (PARTITION BY release_date_dayofweek, is_explicit), 2) AS release_date_dayofweek_is_explicit_avg_bpm,
        SUM(release_date_dayofweek) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS release_date_dayofweek_sum_2,
        MAX(bpm) OVER w_release_date_month_artist_id AS release_date_month_artist_id_max_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit, release_date_dayofweek ORDER BY duration_sec DESC), 3) AS duration_sec_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit, release_date_dayofweek ORDER BY duration_sec DESC), 3) AS duration_sec_cume_dist,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY MONTH(release_date), artist_id), 2) AS duration_sec_vs_release_date_month_artist_id_avg,
        MIN(duration_sec) OVER (PARTITION BY MONTH(release_date), artist_id) AS release_date_month_artist_id_min_duration_sec
    FROM scoped
    WINDOW w_release_date_month_artist_id AS (PARTITION BY MONTH(release_date), artist_id)
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    rn,
    distinct_artist_id_in_release_date_dayofweek_is_,
    bpm_cume_dist,
    bpm_share_of_release_date_month_artist_id,
    release_date_dayofweek_max_6_prior,
    second_release_date_dayofweek,
    release_date_dayofweek_is_explicit_avg_bpm,
    release_date_dayofweek_sum_2,
    release_date_month_artist_id_max_bpm,
    duration_sec_percent_rank,
    duration_sec_cume_dist,
    duration_sec_vs_release_date_month_artist_id_avg,
    release_date_month_artist_id_min_duration_sec,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
    DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label_2
FROM windowed
ORDER BY release_date_dayofweek_max_6_prior
