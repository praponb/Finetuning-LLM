WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC, id) = 1
)
-- Multi-level aggregation
SELECT
    country,
    SIZE(COLLECT_SET(city)) AS num_cities,
    APPROX_PERCENTILE(rating, 0.9) AS rating_p90,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS rating_per_london,
    SUM(rating) FILTER (WHERE city = 'Berlin') AS berlin_total_rating,
    SIZE(COLLECT_SET(city)) AS num_cities_2,
    PERCENTILE_APPROX(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles
FROM ranked
GROUP BY country
ORDER BY num_cities_2 DESC
