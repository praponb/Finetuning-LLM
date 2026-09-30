WITH src AS (
    SELECT *, CAST(price AS INT) AS price_int
    FROM products
    WHERE product_name NOT LIKE '%Mak%' AND brand = 'Stark'
),
agg AS (
    SELECT category, brand, MIN(launch_date) AS min_launch_date
    FROM src
    GROUP BY category, brand
)
SELECT *
FROM agg
ORDER BY category
