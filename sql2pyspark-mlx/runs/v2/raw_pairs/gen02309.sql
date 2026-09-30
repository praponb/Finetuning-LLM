SELECT
    id,
    order_id,
    unit_price,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY unit_price) AS dense_rnk
FROM order_items;
