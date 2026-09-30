WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        DATE_ADD(release_date, 90) AS release_date_plus_90d
    FROM tracks
    WHERE (LENGTH(TRIM(title)) >= 4 OR duration_sec >= 142)
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        release_date_plus_90d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS row_num
        FROM scoped_tracks
    ) ranked
    WHERE row_num = 1
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.release_date_plus_90d,
        a.country AS artist_country,
        a.artist_name
    FROM latest_per_key s
    LEFT JOIN artists a ON a.id = s.artist_id
),
artist_id_stats AS (
    -- Max bpm per artist_id
    SELECT artist_id, MAX(bpm) AS max_bpm
    FROM with_details_artist
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_bpm, ROUND(s.bpm - g.max_bpm, 2) AS bpm_minus_max
    FROM with_details_artist s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm = g.max_bpm
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre,
        a.artist_name AS artist_artist_name
    FROM outliers s
    JOIN artists a ON a.id = s.artist_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        artist_is_verified,
        artist_genre,
        SUM(max_bpm) AS total_max_bpm,
        COUNT(*) AS n_rows,
        ROUND(AVG(bpm_minus_max), 2) AS avg_bpm_minus_max
    FROM combined_details_artist
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE avg_bpm_minus_max > (SELECT AVG(avg_bpm_minus_max) FROM rollup_base)
)
SELECT
    artist_is_verified,
    artist_genre,
    n_rows,
    avg_bpm_minus_max
FROM without_matches
ORDER BY avg_bpm_minus_max NULLS LAST
