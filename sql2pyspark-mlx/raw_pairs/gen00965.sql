SELECT
    id,
    product_id,
    quantity,
    AVG(quantity) OVER w_grp AS last7_avg_quantity,
    MAX(quantity) OVER w_grp AS last7_max_quantity,
    MIN(quantity) OVER w_grp AS last7_min_quantity,
    COUNT(quantity) OVER w_grp AS last7_count_quantity
FROM order_items
WINDOW w_grp AS (PARTITION BY order_id, product_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
