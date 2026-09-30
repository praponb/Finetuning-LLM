SELECT
    id,
    order_id,
    product_id,
    unit_price,
    SUM(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
    AVG(unit_price) OVER w_running AS unit_price_avg_3,
    RANK() OVER w_grp AS unit_price_rank,
    SUM(quantity) OVER w_recent AS cumulative_quantity_2
FROM order_items
WHERE unit_price <= 280
WINDOW w_running AS (PARTITION BY product_id, order_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY order_id ORDER BY unit_price DESC), w_recent AS (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
