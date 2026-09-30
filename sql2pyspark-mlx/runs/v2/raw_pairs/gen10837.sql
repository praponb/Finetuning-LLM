WITH valid_order_items AS (
    SELECT *
    FROM order_items
    WHERE product_id IS NULL
),
Agg AS (
    SELECT product_id, COUNT(*) AS order_item_count
    FROM valid_order_items
    GROUP BY product_id
)
SELECT *
FROM Agg
ORDER BY order_item_count
