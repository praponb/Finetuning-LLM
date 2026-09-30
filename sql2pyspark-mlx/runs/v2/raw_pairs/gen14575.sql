WITH valid_order_items AS (
    SELECT *, CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 2 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM order_items
    WHERE unit_price BETWEEN 150 AND 180
),
OrderItemsStats AS (
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM valid_order_items
    GROUP BY product_id
)
SELECT product_id, total_quantity
FROM OrderItemsStats
WHERE total_quantity < 32
ORDER BY total_quantity, product_id
LIMIT 5;
