WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        bpm - duration_sec AS bpm_minus_duration_sec,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM recent_tracks
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(least_bpm_duration_sec / NULLIF(SUM(least_bpm_duration_sec) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS least_bpm_duration_sec_share_of_artist_id_is_exp,
        MAX(bpm) OVER (PARTITION BY is_explicit ORDER BY least_bpm_duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS bpm_max_2_prior,
        RANK() OVER (PARTITION BY artist_id ORDER BY bpm_minus_duration_sec ASC) AS bpm_minus_duration_sec_rank,
        LEAD(release_date_week) OVER w_artist_id_by_bpm_minus_durat_2 AS next_release_date_week,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY least_bpm_duration_sec), 3) AS least_bpm_duration_sec_percent_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm, id) AS bpm_dense_rank
    FROM scored
    WINDOW w_artist_id_by_bpm_minus_durat_2 AS (PARTITION BY artist_id ORDER BY bpm_minus_duration_sec ASC, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE bpm_dense_rank = 1
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.release_date_week,
        s.bpm_filled,
        s.bpm_minus_duration_sec,
        s.release_date_label,
        s.least_bpm_duration_sec,
        s.least_bpm_duration_sec_share_of_artist_id_is_exp,
        s.next_release_date_week,
        s.least_bpm_duration_sec_percent_rank,
        s.bpm_dense_rank,
        a.artist_name,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM top_rows s
    LEFT JOIN artists a ON a.id = s.artist_id
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        id,
        is_explicit,
        release_date_week,
        bpm_filled,
        bpm_minus_duration_sec,
        release_date_label,
        least_bpm_duration_sec,
        least_bpm_duration_sec_share_of_artist_id_is_exp,
        next_release_date_week,
        bpm_dense_rank,
        artist_name,
        artist_is_verified,
        artist_genre,
        LENGTH(TRIM(artist_name)) AS artist_name_len,
        ROUND(duration_sec / NULLIF(bpm_minus_duration_sec, 0), 3) AS duration_sec_per_bpm_minus_duration_sec,
        TO_DATE(release_date_week) AS release_date_week_date
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, duration_sec_per_bpm_minus_duration_sec, least_bpm_duration_sec_share_of_artist_id_is_exp
    FROM enriched_2
) src
UNPIVOT (
    metric_value FOR metric_name IN (duration_sec_per_bpm_minus_duration_sec, least_bpm_duration_sec_share_of_artist_id_is_exp)
) unp
