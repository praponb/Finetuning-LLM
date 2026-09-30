WITH valid_products AS (
    SELECT *
    FROM products
    WHERE stock_qty <= 169 AND ARRAY_CONTAINS(tags, 'premium')
),
summary AS (
    SELECT brand, MIN(cost) AS min_cost, MIN(launch_date) AS first_launch_date
    FROM valid_products
    GROUP BY brand
)
SELECT brand, min_cost
FROM summary
ORDER BY brand DESC;
