SELECT
    id,
    order_id,
    unit_price,
    product_id,
    quantity,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS rn,
    ROUND(AVG(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_7
FROM order_items
WHERE product_id = 7
ORDER BY order_id DESC;
