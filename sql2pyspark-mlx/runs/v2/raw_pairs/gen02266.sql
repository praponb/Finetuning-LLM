SELECT
    id,
    order_id,
    unit_price,
    product_id,
    LAST_VALUE(product_id) OVER (PARTITION BY order_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_product_id
FROM order_items
