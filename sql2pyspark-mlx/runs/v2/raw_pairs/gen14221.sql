WITH src AS (
    SELECT *
    FROM products
    WHERE cost >= 320 OR category <> 'Grocery'
),
products_stats AS (
    SELECT category, SUM(CASE WHEN brand = 'Initech' THEN price ELSE 0 END) AS initech_price, SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END) AS umbrella_count
    FROM src
    GROUP BY category
)
SELECT category, initech_price, umbrella_count
FROM products_stats
ORDER BY initech_price DESC, category
LIMIT 20;
