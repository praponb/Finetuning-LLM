WITH matched_keys AS (
    SELECT city, id
    FROM suppliers
    INTERSECT ALL
    SELECT city, id
    FROM suppliers
    WHERE supplier_name LIKE '%Paci%'
),
kept_rows AS (
    SELECT s.*
    FROM suppliers s
    INNER JOIN matched_keys k ON s.city = k.city AND s.id = k.id
),
scored AS (
    SELECT
        *,
        STDDEV_SAMP(rating) OVER (PARTITION BY city) AS city_stddev_samp_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share_2,
        SUM(rating) OVER (PARTITION BY city) AS city_sum_rating,
        SUM(rating) OVER (PARTITION BY country) AS country_sum_rating
    FROM kept_rows
),
with_windows AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY country ORDER BY country_sum_rating DESC) AS country_sum_rating_rank,
        MIN(country_sum_rating) OVER (PARTITION BY city) AS city_min_country_sum_rating
    FROM scored
    WHERE country = 'DE'
),
cleaned AS (
    SELECT
        id,
        country,
        rating,
        city_stddev_samp_rating,
        rating_share,
        city_sum_rating,
        country_sum_rating,
        city_min_country_sum_rating,
        rating - city_stddev_samp_rating AS rating_minus_city_stddev_samp_rating,
        CAST(city_min_country_sum_rating AS STRING) AS city_min_country_sum_rating_num
    FROM with_windows
    WHERE supplier_name NOT LIKE 'T%'
),
valid AS (
    SELECT *, ROUND(country_sum_rating / rating, 2) AS country_sum_rating_per_rating
    FROM cleaned
    WHERE rating <= 2.4
)
SELECT
    CASE WHEN rating_minus_city_stddev_samp_rating >= 75 THEN 'LARGE' ELSE 'SMALL' END AS rating_minus_city_stddev_samp_rating_tier,
    country,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(*) AS num_rows,
    MAX(country_sum_rating_per_rating) AS max_country_sum_rating_per_rating,
    SIZE(COLLECT_SET(country)) AS num_countries_2,
    ROUND(AVG(rating_minus_city_stddev_samp_rating), 2) AS avg_rating_minus_city_stddev_samp_rating
FROM valid
GROUP BY rating_minus_city_stddev_samp_rating_tier, country
