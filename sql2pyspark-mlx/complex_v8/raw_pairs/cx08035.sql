WITH valid AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE genre IS NOT NULL
      AND is_verified = FALSE
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(debut_year, 2) OVER (ORDER BY debut_year_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        ROUND(debut_year_filled / NULLIF(SUM(debut_year_filled) OVER w_is_verified, 0), 4) AS debut_year_filled_share_of_is_verified,
        NTILE(5) OVER (ORDER BY debut_year_filled DESC, id) AS debut_year_filled_quintile,
        ROUND(AVG(debut_year_filled) OVER (ORDER BY debut_year_filled DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS debut_year_filled_avg_2
    FROM valid
    WINDOW w_is_verified AS (PARTITION BY is_verified)
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM ranked s
    WHERE s.id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.bpm >= 104
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        ABS(second_debut_year - 2006) AS second_debut_year_distance,
        ROUND(second_debut_year / NULLIF(debut_year_filled, 0), 3) AS second_debut_year_per_debut_year_filled
    FROM without_matches
)
SELECT
    id,
    artist_name,
    debut_year,
    is_verified,
    second_debut_year,
    debut_year_filled_quintile,
    debut_year_filled_avg_2,
    country_or_unknown,
    second_debut_year_per_debut_year_filled,
    LOWER(TRIM(country_or_unknown)) AS country_or_unknown_clean
FROM scored
WHERE artist_name IS NOT NULL
ORDER BY debut_year, country_or_unknown_clean NULLS LAST
