WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE bpm <= 147
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.is_explicit_label,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.country IN ('BR', 'TH')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        bpm,
        is_explicit_label,
        artist_country,
        artist_debut_year,
        ROUND(AVG(artist_debut_year) OVER (PARTITION BY artist_country), 2) AS artist_country_avg_artist_debut_year,
        FIRST_VALUE(artist_debut_year) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year
    FROM combined_details
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    COALESCE(is_explicit_label, 'ALL') AS is_explicit_label,
    ROUND(AVG(artist_debut_year), 2) AS avg_artist_debut_year,
    SUM(CASE WHEN artist_country IS NOT NULL THEN 1 ELSE 0 END) AS n_artist_country_is_not_null,
    ARRAY_SORT(COLLECT_SET(artist_country)) AS artist_country_list
FROM with_trends
GROUP BY ROLLUP(artist_id, is_explicit_label)
ORDER BY artist_id NULLS LAST, is_explicit_label NULLS LAST
