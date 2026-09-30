SELECT
    *,
    NTILE(3) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS unit_price_bucket,
    LAG(quantity, 2, 0) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS prev_quantity,
    MIN(unit_price) OVER (PARTITION BY product_id, order_id) AS product_id_min_unit_price
FROM order_items;
