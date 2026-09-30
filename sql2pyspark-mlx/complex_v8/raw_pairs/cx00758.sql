WITH scoped AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug,
        LOWER(REGEXP_REPLACE(TRIM(genre), ' ', '_')) AS genre_slug
    FROM artists
    WHERE debut_year <= 2018
),
rollup_base AS (
    -- Summarise per group
    SELECT
        genre,
        is_verified,
        country,
        COUNT(*) AS row_count,
        ROUND(AVG(debut_year), 2) AS avg_debut_year,
        SUM(debut_year) AS total_debut_year,
        COUNT(DISTINCT country) AS distinct_countrys,
        COUNT(DISTINCT genre) AS distinct_genres
    FROM scoped
    WHERE genre <> 'Pop'
    GROUP BY 1, 2, 3
    HAVING MAX(debut_year) > 1996
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(total_debut_year, 0) AS total_debut_year_filled,
        CONCAT_WS(': ', genre, country) AS genre_country_label
    FROM rollup_base
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(avg_debut_year - 2003.2) AS avg_debut_year_distance,
        FLOOR(distinct_countrys / 2) * 2 AS distinct_countrys_bucket,
        CASE WHEN genre IN ('Pop', 'Jazz') THEN 'group_a' WHEN genre = 'HipHop' THEN 'group_b' ELSE 'other' END AS genre_group
    FROM derived
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        genre,
        is_verified,
        country,
        row_count,
        total_debut_year,
        distinct_genres,
        avg_debut_year_distance,
        distinct_countrys_bucket,
        genre_group,
        RANK() OVER (PARTITION BY genre_group ORDER BY distinct_countrys DESC) AS distinct_countrys_rank,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY distinct_countrys, genre, is_verified, country) AS distinct_countrys_seq,
        ROUND(distinct_countrys_bucket - AVG(distinct_countrys_bucket) OVER (PARTITION BY is_verified), 2) AS distinct_countrys_bucket_vs_is_verified_avg,
        COUNT(*) OVER (PARTITION BY is_verified) AS rows_in_is_verified
    FROM scored
)
SELECT
    genre,
    is_verified,
    country,
    row_count,
    total_debut_year,
    avg_debut_year_distance,
    distinct_countrys_bucket,
    genre_group,
    distinct_countrys_rank,
    distinct_countrys_bucket_vs_is_verified_avg,
    LENGTH(TRIM(genre_group)) AS genre_group_len,
    LOWER(TRIM(country)) AS country_clean
FROM with_windows
WHERE row_count <= 8
ORDER BY distinct_countrys_bucket_vs_is_verified_avg DESC
