WITH filtered AS (
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
        LENGTH(TRIM(title)) AS title_len,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'R%'
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.title_len,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM filtered s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.country IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        DENSE_RANK() OVER (PARTITION BY artist_id, artist_is_verified ORDER BY bpm DESC) AS bpm_dense_rank,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        title_len - LAG(title_len) OVER w_artist_is_verified_by_releas AS title_len_change,
        FIRST_VALUE(artist_debut_year) OVER (PARTITION BY artist_id, artist_is_verified ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_artist_debut_year
    FROM with_parent_artist
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_is_verified_by_releas AS (PARTITION BY artist_is_verified ORDER BY release_date DESC, id)
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    title_len,
    artist_is_verified,
    bpm_dense_rank,
    bpm_share_of_artist_id,
    artist_id_max_duration_sec,
    rows_in_artist_id,
    first_artist_debut_year,
    artist_id_avg_artist_debut_year
FROM with_windows
ORDER BY release_date, title_len DESC
