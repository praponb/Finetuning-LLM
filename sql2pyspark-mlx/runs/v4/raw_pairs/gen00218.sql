WITH valid AS (
    SELECT *, CONCAT(supplier_name, ' - ', city) AS supplier_name_city
    FROM suppliers
    WHERE rating IS NOT NULL
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    supplier_name_city,
    LAST_VALUE(rating) IGNORE NULLS OVER w_running AS last_known_rating,
    FIRST_VALUE(rating, TRUE) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating,
    SUM(rating) OVER w_running AS running_rating
FROM valid
WINDOW w_running AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
