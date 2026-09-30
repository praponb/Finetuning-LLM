WITH valid_order_items AS (
    SELECT *
    FROM order_items
    WHERE product_id IN (4, 12, 28)
),
agg AS (
    SELECT order_id, SUM(unit_price * quantity) AS revenue
    FROM valid_order_items
    GROUP BY order_id
)
SELECT order_id, revenue
FROM agg
ORDER BY order_id DESC
