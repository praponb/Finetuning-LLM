WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE LOWER(title) LIKE '%ap%'
),
artist_id_stats AS (
    -- Median bpm per artist_id
    SELECT artist_id, PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
    FROM base
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_bpm, ROUND(s.bpm - g.median_bpm, 2) AS bpm_minus_median
    FROM base s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm >= g.median_bpm
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.title_prefix,
        a.debut_year AS artist_debut_year
    FROM vs_group s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    title_prefix,
    artist_debut_year,
    UPPER(TRIM(title_prefix)) AS title_prefix_clean,
    CONCAT(title_prefix, '#', CAST(artist_id AS STRING)) AS title_prefix_ref
FROM joined
ORDER BY title_prefix NULLS LAST
