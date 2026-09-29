WITH filtered_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        IF(is_verified, 'yes', 'no') AS is_verified_label,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        CONCAT_WS(': ', country, artist_name) AS country_artist_name_label
    FROM artists
),
summary AS (
    -- Summarise per group
    SELECT
        genre,
        is_verified_label,
        is_verified_flag,
        MAX(debut_year) AS max_debut_year,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
        ARRAY_SORT(COLLECT_SET(country)) AS country_list,
        COUNT(*) AS cnt
    FROM filtered_artists
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
flattened AS (
    -- One row per country_list_item
    SELECT
        s.genre,
        s.is_verified_label,
        s.is_verified_flag,
        s.max_debut_year,
        s.distinct_ids,
        s.artist_name_list,
        s.cnt,
        t.country_list_item
    FROM summary s
    LATERAL VIEW OUTER EXPLODE(s.country_list) t AS country_list_item
),
flattened_2 AS (
    -- One row per artist_name_list_item
    SELECT
        s.genre,
        s.is_verified_label,
        s.is_verified_flag,
        s.max_debut_year,
        s.distinct_ids,
        s.cnt,
        s.country_list_item,
        t.artist_name_list_item
    FROM flattened s
    LATERAL VIEW EXPLODE(s.artist_name_list) t AS artist_name_list_item
)
SELECT
    genre,
    is_verified_label,
    is_verified_flag,
    max_debut_year,
    distinct_ids,
    country_list_item,
    artist_name_list_item,
    CASE WHEN genre IN ('Classical', 'HipHop') THEN 'group_a' WHEN genre = 'Pop' THEN 'group_b' ELSE 'other' END AS genre_group,
    max_debut_year + cnt AS max_debut_year_plus_cnt
FROM flattened_2
ORDER BY country_list_item DESC NULLS FIRST, artist_name_list_item
