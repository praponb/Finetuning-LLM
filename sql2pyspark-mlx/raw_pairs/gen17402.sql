WITH base AS (
    SELECT
        id,
        country,
        city,
        rating,
        CONCAT_WS('-', supplier_name, city) AS supplier_name_city_key,
        CONCAT_WS('-', country, supplier_name) AS country_supplier_name_key
    FROM suppliers
    WHERE city IS NOT NULL
)
SELECT
    id,
    country,
    city,
    rating,
    country_supplier_name_key,
    AVG(rating) OVER w_hist AS rating_avg_2,
    LAG(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating,
    NTILE(3) OVER (ORDER BY rating DESC, id) AS rating_bucket
FROM base
WINDOW w_hist AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
