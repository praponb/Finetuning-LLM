WITH filtered_order_items AS (
    SELECT *
    FROM order_items
    WHERE quantity >= 3
),
agg AS (
    SELECT order_id, SUM(unit_price * quantity) AS revenue
    FROM filtered_order_items
    GROUP BY order_id
)
SELECT order_id, revenue
FROM agg
