WITH valid_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        COALESCE(debut_year, 0) AS debut_year_filled,
        LOWER(TRIM(country)) AS country_clean
    FROM artists
    WHERE debut_year >= 1997
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM valid_artists s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND NOT t.is_explicit
              AND t.bpm <= 156
        )
      AND s.country = 'JP'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', country, artist_name) AS country_artist_name_label,
        SUBSTRING(TRIM(country), 1, 3) AS country_prefix,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown
    FROM without_matches
    WHERE debut_year_filled < 2020
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY genre_or_unknown, genre ORDER BY debut_year_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(debut_year_filled) OVER (PARTITION BY genre_or_unknown, genre ORDER BY debut_year_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_debut_year_filled,
        NTH_VALUE(debut_year_filled, 2) OVER (PARTITION BY genre_or_unknown, genre ORDER BY debut_year_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year_filled,
        MIN(debut_year_filled) OVER w_genre_genre_or_unknown AS genre_genre_or_unknown_min_debut_year_filled,
        ROUND(debut_year_filled - AVG(debut_year_filled) OVER (PARTITION BY genre, genre_or_unknown), 2) AS debut_year_filled_vs_genre_genre_or_unknown_avg,
        ROW_NUMBER() OVER (PARTITION BY genre_or_unknown, genre ORDER BY debut_year_filled DESC, id) AS seq_no
    FROM tagged
    WINDOW w_genre_genre_or_unknown AS (PARTITION BY genre, genre_or_unknown)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year_filled,
        country_clean,
        country_artist_name_label,
        country_prefix,
        running_count,
        debut_year_filled_vs_genre_genre_or_unknown_avg,
        seq_no,
        SUM(second_debut_year_filled) OVER (PARTITION BY genre_or_unknown) AS genre_or_unknown_sum_second_debut_year_filled,
        ROW_NUMBER() OVER (PARTITION BY seq_no, genre_or_unknown ORDER BY debut_year_filled, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY country, genre_or_unknown ORDER BY first_debut_year_filled ASC), 3) AS first_debut_year_filled_percent_rank
    FROM with_windows
),
grouped AS (
    -- Summarise per group
    SELECT
        country,
        genre,
        rn,
        COUNT(*) AS row_count,
        COUNT(DISTINCT country) AS distinct_countrys,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list
    FROM with_trends
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    country,
    genre,
    row_count,
    artist_name_list,
    CASE WHEN genre IN ('Pop', 'Jazz') THEN 'group_a' WHEN genre = 'Classical' THEN 'group_b' ELSE 'other' END AS genre_group,
    CASE WHEN row_count >= 8 THEN 'HIGH' WHEN row_count >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS row_count_tier
FROM grouped
WHERE genre IS NOT NULL
ORDER BY country
