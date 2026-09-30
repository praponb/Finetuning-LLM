WITH recent_products AS (
    SELECT *
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'clearance') OR price >= 760
),
Summary AS (
    SELECT brand, AVG(cost) AS avg_cost, COUNT(price) AS price_count
    FROM recent_products
    GROUP BY brand
)
SELECT brand, avg_cost, price_count
FROM Summary
WHERE avg_cost >= 440
