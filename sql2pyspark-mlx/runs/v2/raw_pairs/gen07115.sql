SELECT
    id,
    order_id,
    unit_price,
    quantity,
    product_id,
    LAG(quantity, 2) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_quantity,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 3) AS unit_price_share
FROM order_items
