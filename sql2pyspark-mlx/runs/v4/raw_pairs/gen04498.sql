SELECT
    id,
    supplier_id,
    cost,
    AVG(cost) OVER (PARTITION BY supplier_id) AS supplier_id_avg_cost
FROM products
ORDER BY supplier_id_avg_cost DESC
