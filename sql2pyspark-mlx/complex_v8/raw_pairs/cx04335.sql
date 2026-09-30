WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
      AND bpm BETWEEN 73 AND 180
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.title_ref,
        s.title_clean,
        a.debut_year AS artist_debut_year
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title_ref,
        artist_debut_year,
        ROW_NUMBER() OVER (ORDER BY bpm DESC NULLS FIRST, id) AS seq_no,
        NTH_VALUE(artist_debut_year, 2) OVER (ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        MAX(bpm) OVER (PARTITION BY artist_id) AS artist_id_max_bpm,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS artist_debut_year_avg_7,
        LAST_VALUE(bpm) OVER (ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER w_artist_id, 0), 4) AS artist_debut_year_share_of_artist_id,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year_2,
        LEAD(bpm, 2) OVER (ORDER BY bpm NULLS LAST, id) AS next2_bpm,
        artist_debut_year - LAG(artist_debut_year) OVER (PARTITION BY is_explicit, artist_id ORDER BY artist_debut_year, id) AS artist_debut_year_change
    FROM combined_details_artist
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
-- Final projection
SELECT
    id,
    title_ref,
    seq_no,
    artist_id_max_bpm,
    last_bpm,
    artist_debut_year_share_of_artist_id,
    second_artist_debut_year_2,
    artist_debut_year_change
FROM with_trends
ORDER BY second_artist_debut_year_2 NULLS LAST, seq_no DESC, id
LIMIT 20
