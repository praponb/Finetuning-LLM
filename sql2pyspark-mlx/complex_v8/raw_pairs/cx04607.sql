WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref
    FROM tracks
    WHERE bpm BETWEEN 76 AND 170
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        title_ref,
        LAST_VALUE(days_since_release_date) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_release_date,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        ROUND(AVG(days_since_release_date) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_days_since_release_date,
        SUM(days_since_release_date) OVER (PARTITION BY artist_id) AS artist_id_sum_days_since_release_date,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS bpm_vs_is_explicit_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec DESC NULLS FIRST), 3) AS duration_sec_percent_rank,
        ROUND(AVG(bpm) OVER w_is_explicit, 2) AS is_explicit_avg_bpm,
        days_since_release_date - LAG(days_since_release_date) OVER w_all_by_duration_sec AS days_since_release_date_change,
        NTILE(10) OVER (PARTITION BY artist_id, is_explicit ORDER BY days_since_release_date DESC, id) AS days_since_release_date_decile,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec), 3) AS duration_sec_cume_dist,
        SUM(bpm) OVER (ORDER BY duration_sec, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS bpm_sum_2,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        MIN(bpm) OVER (PARTITION BY is_explicit) AS is_explicit_min_bpm,
        LAG(duration_sec, 1, 0) OVER (ORDER BY duration_sec, id) AS prev_duration_sec
    FROM base_tracks
    WINDOW w_is_explicit AS (PARTITION BY is_explicit), w_all_by_duration_sec AS (ORDER BY duration_sec, id)
)
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    title_ref,
    last_known_days_since_release_date,
    is_explicit_avg_days_since_release_date,
    artist_id_sum_days_since_release_date,
    bpm_vs_is_explicit_avg,
    duration_sec_percent_rank,
    is_explicit_avg_bpm,
    days_since_release_date_change,
    days_since_release_date_decile,
    duration_sec_cume_dist,
    duration_sec_avg_7,
    is_explicit_min_bpm,
    prev_duration_sec
FROM sequenced
ORDER BY artist_id DESC, id DESC
LIMIT 10
