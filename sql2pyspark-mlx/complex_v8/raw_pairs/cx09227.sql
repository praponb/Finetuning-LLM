WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE bpm > 85
      AND is_explicit = FALSE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        moods_text,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        LEAD(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC, id) AS next_duration_sec,
        FIRST_VALUE(release_date) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_duration_sec, 3) AS duration_sec_percent_rank,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, MONTH(release_date)), 2) AS artist_id_release_date_month_avg_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, YEAR(release_date) ORDER BY duration_sec DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior,
        SUM(duration_sec) OVER w_artist_id AS artist_id_sum_duration_sec,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS duration_sec_seq,
        DENSE_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC) AS release_date_dense_rank
    FROM recent_tracks
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec DESC, id), w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    artist_id,
    bpm,
    moods,
    next_duration_sec,
    first_release_date,
    artist_id_release_date_month_avg_bpm,
    artist_id_sum_duration_sec,
    duration_sec_seq
FROM with_trends
WHERE release_date_dense_rank < 7
ORDER BY artist_id_release_date_month_avg_bpm, first_release_date NULLS LAST
