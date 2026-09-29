WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
    WHERE is_explicit = FALSE
      AND NOT ARRAY_CONTAINS(moods, 'sad')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        days_since_release_date,
        is_explicit_flag,
        ROW_NUMBER() OVER (PARTITION BY is_explicit_flag, is_explicit ORDER BY bpm, id) AS bpm_seq,
        ROUND(is_explicit_flag / NULLIF(SUM(is_explicit_flag) OVER (PARTITION BY MONTH(release_date), is_explicit), 0), 4) AS is_explicit_flag_share_of_release_date_month_is_,
        NTILE(5) OVER w_artist_id_is_explicit_flag_b AS duration_sec_quintile,
        DATEDIFF(release_date, LAG(release_date) OVER (ORDER BY days_since_release_date DESC, id)) AS days_since_prev_release_date,
        ROUND(is_explicit_flag / NULLIF(SUM(is_explicit_flag) OVER (PARTITION BY MONTH(release_date), artist_id), 0), 4) AS is_explicit_flag_share_of_release_date_month_art,
        LEAD(duration_sec, 3) OVER (ORDER BY days_since_release_date DESC, id) AS next3_duration_sec,
        COUNT(*) OVER w_release_date_month_artist_id AS rows_in_release_date_month_artist_id,
        DENSE_RANK() OVER (PARTITION BY is_explicit_flag, is_explicit ORDER BY bpm) AS bpm_dense_rank
    FROM base_tracks
    WINDOW w_artist_id_is_explicit_flag_b AS (PARTITION BY artist_id, is_explicit_flag ORDER BY duration_sec NULLS LAST, id), w_release_date_month_artist_id AS (PARTITION BY MONTH(release_date), artist_id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        days_since_release_date,
        is_explicit_flag,
        bpm_seq,
        is_explicit_flag_share_of_release_date_month_is_,
        duration_sec_quintile,
        days_since_prev_release_date,
        is_explicit_flag_share_of_release_date_month_art,
        next3_duration_sec,
        rows_in_release_date_month_artist_id,
        LAG(duration_sec, 3) OVER (ORDER BY rows_in_release_date_month_artist_id ASC, id) AS prev3_duration_sec,
        next3_duration_sec - LAG(next3_duration_sec) OVER (ORDER BY rows_in_release_date_month_artist_id ASC, id) AS next3_duration_sec_change,
        RANK() OVER (PARTITION BY artist_id, bpm_dense_rank ORDER BY is_explicit_flag DESC) AS is_explicit_flag_rank,
        MIN(bpm) OVER w_artist_id_release_date_year AS artist_id_release_date_year_min_bpm,
        DENSE_RANK() OVER (PARTITION BY bpm_seq, is_explicit_flag ORDER BY next3_duration_sec, id) AS next3_duration_sec_dense_rank,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY next3_duration_sec DESC, id) AS next3_duration_sec_dense_rank_2,
        ROUND(is_explicit_flag / NULLIF(SUM(is_explicit_flag) OVER (PARTITION BY artist_id, YEAR(release_date)), 0), 4) AS is_explicit_flag_share_of_artist_id_release_date
    FROM with_trends
    WINDOW w_artist_id_release_date_year AS (PARTITION BY artist_id, YEAR(release_date))
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    is_explicit_flag,
    bpm_seq,
    is_explicit_flag_share_of_release_date_month_is_,
    days_since_prev_release_date,
    rows_in_release_date_month_artist_id,
    next3_duration_sec_change,
    is_explicit_flag_rank,
    artist_id_release_date_year_min_bpm,
    next3_duration_sec_dense_rank,
    next3_duration_sec_dense_rank_2,
    duration_sec_quintile - duration_sec AS duration_sec_quintile_minus_duration_sec,
    DATE_TRUNC('MONTH', release_date) AS release_date_month
FROM metrics
