SELECT
    id,
    order_id,
    product_id,
    quantity,
    AVG(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_unit_price,
    SUM(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price,
    MAX(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_unit_price,
    RANK() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC NULLS LAST) AS unit_price_rank
FROM order_items
WHERE order_id = 27
