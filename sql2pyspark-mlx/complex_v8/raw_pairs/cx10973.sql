WITH cleaned_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        debut_year,
        is_verified,
        LENGTH(TRIM(country)) AS country_len,
        CASE WHEN debut_year >= 2016 THEN 'large' WHEN debut_year >= 1997 THEN 'medium' ELSE 'small' END AS debut_year_tier,
        CASE WHEN debut_year >= 2018 THEN 'large' WHEN debut_year >= 1997 THEN 'medium' ELSE 'small' END AS debut_year_tier_2
    FROM artists
    WHERE country NOT IN ('KR', 'BR')
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned_artists
    WHERE debut_year > (SELECT AVG(debut_year) FROM cleaned_artists)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(country_len, 3) OVER (ORDER BY country_len ASC NULLS LAST, id) AS prev3_country_len,
        ROUND(debut_year - AVG(debut_year) OVER (PARTITION BY debut_year_tier_2), 2) AS debut_year_vs_debut_year_tier_2_avg
    FROM filtered
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_name,
        debut_year,
        is_verified,
        country_len,
        debut_year_tier,
        debut_year_vs_debut_year_tier_2_avg,
        RANK() OVER (ORDER BY country_len DESC) AS country_len_rank,
        ROUND(AVG(prev3_country_len) OVER (ORDER BY country_len DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS prev3_country_len_avg_4,
        SUM(prev3_country_len) OVER (ORDER BY country_len DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev3_country_len_sum_2_prior
    FROM windowed
)
SELECT id, metric, value
FROM (
    SELECT id, country_len, prev3_country_len_sum_2_prior
    FROM ranked
) src
UNPIVOT (
    value FOR metric IN (country_len AS country_len_v, prev3_country_len_sum_2_prior AS m_prev3_country_len_sum_2_prior)
) unp
