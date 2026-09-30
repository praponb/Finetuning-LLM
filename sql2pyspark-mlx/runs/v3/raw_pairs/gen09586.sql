WITH ranked_order_items AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS rn
    FROM order_items
    WHERE unit_price >= 130
)
SELECT
    id,
    order_id,
    unit_price,
    quantity
FROM ranked_order_items
WHERE rn <= 3
