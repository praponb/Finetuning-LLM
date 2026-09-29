WITH base AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CONCAT(city, ', ', country) AS city_country,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM suppliers
    WHERE rating IS NOT NULL
)
SELECT
    id,
    supplier_name,
    city,
    rating,
    city_country,
    city_filled,
    FIRST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
    AVG(rating) OVER w_grp AS rating_avg_3_prior
FROM base
WINDOW w_grp AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
