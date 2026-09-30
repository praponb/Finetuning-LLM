SELECT
    id,
    supplier_id,
    cost,
    RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC) AS cost_rank
FROM products
