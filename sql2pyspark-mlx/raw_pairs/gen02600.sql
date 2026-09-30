SELECT
    id,
    product_id,
    quantity,
    MIN(quantity) OVER w_grp AS running_min_quantity,
    AVG(quantity) OVER w_grp AS running_avg_quantity,
    MAX(quantity) OVER w_grp AS running_max_quantity
FROM order_items
WINDOW w_grp AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
