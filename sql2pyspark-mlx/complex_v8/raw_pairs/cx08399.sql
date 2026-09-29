WITH recent_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket_2
    FROM artists
    WHERE country = 'JP'
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        artist_name,
        debut_year_bucket,
        debut_year_bucket_2,
        debut_year + debut_year_bucket AS debut_year_plus_debut_year_bucket,
        IF(is_verified, 'yes', 'no') AS is_verified_label,
        debut_year + debut_year_bucket_2 AS debut_year_plus_debut_year_bucket_2
    FROM recent_artists
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(debut_year_bucket_2) OVER (PARTITION BY is_verified_label ORDER BY debut_year_bucket_2 ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS debut_year_bucket_2_max_2,
        FIRST_VALUE(debut_year_bucket_2) IGNORE NULLS OVER (PARTITION BY is_verified_label ORDER BY debut_year_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_debut_year_bucket_2,
        ROW_NUMBER() OVER w_is_verified_label_by_debut_y_3 AS seq_no,
        ROUND(debut_year_bucket - AVG(debut_year_bucket) OVER w_is_verified_label, 2) AS debut_year_bucket_vs_is_verified_label_avg
    FROM labeled
    WINDOW w_is_verified_label_by_debut_y_3 AS (PARTITION BY is_verified_label ORDER BY debut_year_bucket_2 ASC, id), w_is_verified_label AS (PARTITION BY is_verified_label)
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND NOT ARRAY_CONTAINS(t.moods, 'sad')
        )
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        id,
        debut_year_bucket,
        debut_year_plus_debut_year_bucket,
        debut_year_plus_debut_year_bucket_2,
        debut_year_bucket_2_max_2,
        first_known_debut_year_bucket_2,
        debut_year_bucket_vs_is_verified_label_avg,
        ROUND(debut_year_bucket_2 / NULLIF(debut_year_bucket, 0), 3) AS debut_year_bucket_2_per_debut_year_bucket,
        UPPER(TRIM(is_verified_label)) AS is_verified_label_clean
    FROM filtered
)
-- Final projection
SELECT
    id,
    debut_year_bucket,
    debut_year_plus_debut_year_bucket,
    debut_year_plus_debut_year_bucket_2,
    debut_year_bucket_2_max_2,
    debut_year_bucket_2_per_debut_year_bucket,
    SUBSTRING(TRIM(is_verified_label_clean), 1, 4) AS is_verified_label_clean_prefix,
    SUBSTRING(TRIM(is_verified_label_clean), 1, 3) AS is_verified_label_clean_prefix_2
FROM labeled_2
WHERE LENGTH(TRIM(is_verified_label_clean)) >= 5
ORDER BY debut_year_plus_debut_year_bucket_2 NULLS LAST, id
LIMIT 5
