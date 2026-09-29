WITH supplier_totals AS (
    SELECT supplier_id, MAX(cost) - MIN(cost) AS cost_range, SUM(CASE WHEN brand = 'Acme' THEN price ELSE 0 END) AS acme_price
    FROM products
    GROUP BY supplier_id
)
SELECT
    suppliers.id,
    suppliers.rating,
    supplier_totals.cost_range,
    supplier_totals.acme_price
FROM suppliers
INNER JOIN supplier_totals ON suppliers.id = supplier_totals.supplier_id
WHERE supplier_totals.cost_range >= 80;
