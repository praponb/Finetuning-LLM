WITH top_order_items AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rnk
    FROM order_items
    WHERE order_id = 39
)
SELECT *
FROM top_order_items
WHERE rnk = 1
ORDER BY id DESC
