SELECT
    id,
    product_id,
    unit_price,
    NTILE(4) OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS quartile
FROM order_items;
