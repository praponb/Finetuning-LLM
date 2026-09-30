SELECT
    id,
    order_id,
    product_id,
    unit_price,
    DENSE_RANK() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC) AS dense_rnk
FROM order_items
ORDER BY unit_price ASC;
