WITH base AS (
    SELECT *
    FROM order_items
    WHERE product_id IS NOT NULL
),
OrderItemsStats AS (
    SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, SUM(quantity) AS total_quantity, MIN(unit_price) AS min_unit_price
    FROM base
    GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
)
SELECT quantity_bucket, total_quantity, min_unit_price
FROM OrderItemsStats
ORDER BY total_quantity DESC, quantity_bucket DESC
LIMIT 10;
