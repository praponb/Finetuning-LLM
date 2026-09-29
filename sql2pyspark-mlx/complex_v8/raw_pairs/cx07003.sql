WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 7) AS release_date_plus_7d
    FROM tracks
    WHERE artist_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date_plus_7d)) AS rows_in_is_explicit_release_date_plus_7d_month,
        NTILE(10) OVER (ORDER BY release_date_plus_7d, id) AS release_date_plus_7d_decile,
        LAST_VALUE(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY release_date) AS release_date_dense_rank,
        MAX(bpm) OVER w_is_explicit_release_date_plu AS is_explicit_release_date_plus_7d_month_max_bpm,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date_plus_7d)), 0), 4) AS duration_sec_share_of_is_explicit_release_date_p,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date, id) AS duration_sec_change,
        RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST) AS duration_sec_rank,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit_release_date_plu) AS distinct_artist_id_in_is_explicit_release_date_p
    FROM cleaned_tracks
    WINDOW w_is_explicit_release_date_plu AS (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date_plus_7d))
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    rows_in_is_explicit_release_date_plus_7d_month,
    last_duration_sec,
    bpm_share_of_artist_id,
    release_date_dense_rank,
    is_explicit_release_date_plus_7d_month_max_bpm,
    duration_sec_share_of_is_explicit_release_date_p,
    duration_sec_change,
    duration_sec_rank,
    distinct_artist_id_in_is_explicit_release_date_p,
    TRUNC(release_date_plus_7d, 'MM') AS release_date_plus_7d_month_start,
    GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec
FROM metrics
ORDER BY bpm_share_of_artist_id DESC NULLS FIRST
