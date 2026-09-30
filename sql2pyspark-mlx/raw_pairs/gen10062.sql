WITH ranked_order_items AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rnk
    FROM order_items
)
SELECT id, order_id, quantity
FROM ranked_order_items
WHERE rnk <= 3;
