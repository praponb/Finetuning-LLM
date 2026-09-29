WITH recent_products AS (
    SELECT *, stock_qty - cost AS stock_qty_minus_cost
    FROM products
    WHERE category = 'Electronics'
),
Agg AS (
    SELECT supplier_id, COUNT(DISTINCT cost) AS unique_suppliers, ROUND((100 * SUM(CASE WHEN brand = 'Globex' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_globex
    FROM recent_products
    GROUP BY supplier_id
)
SELECT supplier_id, unique_suppliers, pct_globex
FROM Agg
WHERE unique_suppliers >= 5
