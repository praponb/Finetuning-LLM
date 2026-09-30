SELECT
    id,
    product_id,
    quantity,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 3) AS quantity_share,
    LEAD(quantity, 2) OVER (ORDER BY quantity, id) AS next_quantity
FROM order_items
