SELECT
    id,
    category,
    supplier_id,
    cost,
    RANK() OVER (PARTITION BY category, supplier_id ORDER BY cost DESC) AS cost_rank
FROM products
WHERE price <= 1090
