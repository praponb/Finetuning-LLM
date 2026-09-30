WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 7) AS release_date_plus_7d,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
      AND artist_id <= 3
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(release_date, release_date_plus_7d) AS days_release_date_plus_7d_to_release_date
    FROM base
    WHERE least_bpm_duration_sec > 204
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre,
        a.artist_name
    FROM scored s
    JOIN artists a ON a.id = s.artist_id
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        release_date_plus_7d,
        title_prefix,
        least_bpm_duration_sec,
        days_release_date_plus_7d_to_release_date,
        artist_is_verified,
        artist_genre,
        artist_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS row_num
        FROM with_parent_artist
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    is_explicit,
    artist_genre,
    artist_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids
FROM latest
GROUP BY CUBE(is_explicit, artist_genre, artist_id)
ORDER BY is_explicit NULLS LAST, artist_genre NULLS LAST, artist_id NULLS LAST
