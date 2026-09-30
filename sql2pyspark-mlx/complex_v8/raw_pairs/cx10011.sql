WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
    WHERE is_explicit = TRUE
),
with_details_artist AS (
    -- Join artists
    SELECT s.*, a.genre AS artist_genre, a.artist_name
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with CUBE
SELECT
    artist_genre,
    artist_id,
    ARRAY_SORT(COLLECT_SET(title)) AS title_list,
    COUNT(*) AS n_rows,
    SUM(bpm) FILTER (WHERE LOWER(title) LIKE '%eo%') AS bpm_lower_title_like_eo
FROM with_details_artist
GROUP BY CUBE(artist_genre, artist_id)
ORDER BY artist_genre NULLS LAST, artist_id NULLS LAST
