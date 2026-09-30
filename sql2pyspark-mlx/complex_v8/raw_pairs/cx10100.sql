WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'S%'
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        least_bpm_duration_sec,
        bpm_per_duration_sec
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS rn
        FROM recent
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        least_bpm_duration_sec,
        bpm_per_duration_sec,
        MAX(bpm_per_duration_sec) OVER (ORDER BY bpm_per_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_per_duration_sec_so_far,
        ROW_NUMBER() OVER (ORDER BY bpm_per_duration_sec ASC, id) AS bpm_per_duration_sec_seq,
        ROUND(AVG(bpm_per_duration_sec) OVER (ORDER BY bpm_per_duration_sec ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_per_duration_sec_avg_3,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id
    FROM deduped
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date,
        (SELECT MAX(t.duration_sec) FROM tracks t WHERE t.artist_id = s.artist_id) AS max_track_duration_sec
    FROM with_windows s
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.least_bpm_duration_sec,
        s.bpm_per_duration_sec,
        s.max_bpm_per_duration_sec_so_far,
        s.bpm_per_duration_sec_seq,
        s.bpm_per_duration_sec_avg_3,
        s.rows_in_artist_id,
        s.last_track_release_date,
        s.max_track_duration_sec,
        t.mood
    FROM with_counts s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.least_bpm_duration_sec,
        s.max_bpm_per_duration_sec_so_far,
        s.bpm_per_duration_sec_seq,
        s.bpm_per_duration_sec_avg_3,
        s.rows_in_artist_id,
        s.last_track_release_date,
        s.max_track_duration_sec,
        s.mood,
        a.artist_name,
        a.genre AS artist_genre
    FROM flattened s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    bpm,
    max_bpm_per_duration_sec_so_far,
    bpm_per_duration_sec_seq,
    bpm_per_duration_sec_avg_3,
    rows_in_artist_id,
    last_track_release_date,
    artist_name,
    artist_genre,
    GREATEST(bpm, bpm_per_duration_sec_seq) AS greatest_bpm_bpm_per_duration_sec_seq,
    DAYOFWEEK(last_track_release_date) AS last_track_release_date_dayofweek
FROM combined_details_artist
ORDER BY bpm_per_duration_sec_seq DESC, artist_id DESC
