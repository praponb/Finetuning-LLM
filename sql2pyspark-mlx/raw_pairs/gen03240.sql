SELECT
    id,
    order_id,
    product_id,
    quantity,
    RANK() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC) rnk
FROM order_items
WHERE quantity > 5;
