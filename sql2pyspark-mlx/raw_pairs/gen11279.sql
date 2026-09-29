WITH base AS (
    SELECT *
    FROM order_items
    WHERE (order_id IN (1, 6, 18, 35) AND unit_price <= 280) OR product_id IN (8, 20, 30, 38)
),
agg AS (
    SELECT product_id, COUNT(*) total_count, SUM(unit_price) total_unit_price
    FROM base
    GROUP BY product_id
)
SELECT *
FROM agg
WHERE total_unit_price >= 1200
ORDER BY total_unit_price DESC, product_id
LIMIT 5;
