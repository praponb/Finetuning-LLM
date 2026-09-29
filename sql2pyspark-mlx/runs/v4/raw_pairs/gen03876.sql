SELECT
    product_id,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    MAX(quantity) AS max_quantity,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
    PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95
FROM inventory
GROUP BY 1
HAVING ROUND(AVG(quantity), 2) >= 0
ORDER BY quantity_p90 NULLS LAST
