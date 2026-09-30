WITH cleaned_artists AS (
    -- Filter artists
    SELECT
        id,
        country,
        debut_year,
        is_verified,
        CASE WHEN genre IN ('Jazz', 'Indie') THEN 'group_a' WHEN genre = 'Pop' THEN 'group_b' ELSE 'other' END AS genre_group
    FROM artists
    WHERE genre IS NOT NULL
      AND LENGTH(TRIM(artist_name)) >= 7
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        country,
        debut_year,
        is_verified,
        ROUND(AVG(debut_year) OVER (ORDER BY debut_year DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS debut_year_avg_4,
        FIRST_VALUE(debut_year) IGNORE NULLS OVER (PARTITION BY is_verified, genre_group ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_debut_year,
        NTH_VALUE(debut_year, 2) OVER (PARTITION BY genre_group ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        ROUND(CUME_DIST() OVER w_is_verified_genre_group_by_d_2, 3) AS debut_year_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY genre_group ORDER BY debut_year DESC, id), 3) AS debut_year_percent_rank
    FROM cleaned_artists
    WINDOW w_is_verified_genre_group_by_d_2 AS (PARTITION BY is_verified, genre_group ORDER BY debut_year)
)
-- Subtotals with ROLLUP
SELECT
    country,
    is_verified,
    GROUPING(country) AS is_total_country,
    MAX(second_debut_year) AS max_second_debut_year,
    ARRAY_SORT(COLLECT_SET(country)) AS country_list,
    SUM(CASE WHEN country <> 'BR' THEN second_debut_year ELSE 0 END) AS second_debut_year_country_br
FROM sequenced
GROUP BY ROLLUP(country, is_verified)
ORDER BY country NULLS LAST, is_verified NULLS LAST
