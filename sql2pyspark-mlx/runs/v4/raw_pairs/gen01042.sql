SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
    SUM(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
    AVG(quantity) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
    ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS row_seq
FROM order_items
