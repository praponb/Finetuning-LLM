SELECT
    id,
    order_id,
    quantity,
    product_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rn
    FROM order_items
    WHERE product_id = 37
) AS ranked
WHERE rn <= 3
ORDER BY order_id DESC;
