WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        duration_sec + bpm AS duration_sec_plus_bpm,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.artist_name, a.debut_year AS artist_debut_year
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY release_date_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year,
        COUNT(*) OVER (PARTITION BY artist_id, YEAR(release_date_week) ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC, id) AS bpm_change,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC, id) AS seq_no,
        LAG(release_date_week) OVER w_artist_id_by_bpm AS prev_release_date_week,
        DENSE_RANK() OVER (ORDER BY bpm) AS bpm_dense_rank,
        MIN(duration_sec_plus_bpm) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec_plus_bpm,
        COUNT(*) OVER (PARTITION BY artist_id ORDER BY release_date_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2
    FROM with_parent_artist
    WINDOW w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm, id)
)
SELECT
    id,
    title,
    moods,
    duration_sec_plus_bpm,
    artist_name,
    artist_debut_year,
    last_artist_debut_year,
    prev_release_date_week,
    bpm_dense_rank,
    running_count_2,
    bpm_change - bpm AS bpm_change_minus_bpm
FROM metrics
ORDER BY last_artist_debut_year, bpm_change_minus_bpm NULLS LAST, id
LIMIT 10
