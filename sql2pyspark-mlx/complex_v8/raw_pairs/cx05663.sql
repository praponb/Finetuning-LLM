WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 7) AS release_date_plus_7d,
        MONTH(release_date) AS release_date_month
    FROM tracks
    WHERE (NOT is_explicit OR bpm < 147)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        bpm,
        moods,
        release_date_plus_7d,
        release_date_month,
        COUNT(*) OVER (PARTITION BY release_date_month, is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY release_date_month)) AS distinct_artist_id_in_release_date_month,
        MAX(release_date_month) OVER (PARTITION BY release_date_month, is_explicit ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS release_date_month_max_2_prior,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id, release_date_month ORDER BY duration_sec, id) AS bpm_change,
        bpm - LAG(bpm) OVER (PARTITION BY release_date_month, is_explicit ORDER BY release_date, id) AS bpm_change_2,
        ROUND(AVG(bpm) OVER (PARTITION BY release_date_month), 2) AS release_date_month_avg_bpm,
        MIN(release_date_month) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_min_release_date_month,
        ROUND(release_date_month - AVG(release_date_month) OVER (PARTITION BY release_date_month), 2) AS release_date_month_vs_release_date_month_avg
    FROM base
)
-- Final projection
SELECT
    id,
    bpm,
    release_date_plus_7d,
    release_date_month,
    running_count,
    distinct_artist_id_in_release_date_month,
    bpm_change,
    bpm_change_2,
    release_date_month_avg_bpm,
    artist_id_is_explicit_min_release_date_month,
    release_date_month_vs_release_date_month_avg,
    DATE_FORMAT(release_date_plus_7d, 'yyyy-MM') AS release_date_plus_7d_label
FROM with_windows
