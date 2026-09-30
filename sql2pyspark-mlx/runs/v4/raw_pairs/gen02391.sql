SELECT
    order_id,
    product_id,
    MAX(unit_price) AS max_unit_price,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95
FROM order_items
GROUP BY order_id, product_id
HAVING PERCENTILE_APPROX(quantity, 0.95) > 0
ORDER BY order_id DESC NULLS FIRST;
