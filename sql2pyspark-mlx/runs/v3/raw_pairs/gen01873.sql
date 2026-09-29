SELECT
    id,
    supplier_id,
    category,
    cost,
    RANK() OVER (PARTITION BY supplier_id, category ORDER BY cost) AS cost_rank
FROM products
ORDER BY supplier_id DESC;
