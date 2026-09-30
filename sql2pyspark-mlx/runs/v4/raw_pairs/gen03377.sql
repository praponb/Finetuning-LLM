WITH recent_products AS (
    SELECT *, cost - stock_qty AS cost_minus_stock_qty
    FROM products
    WHERE stock_qty IS NOT NULL
),
products_stats AS (
    SELECT category, supplier_id, SUM(cost_minus_stock_qty) AS total_cost_minus_stock_qty
    FROM recent_products
    GROUP BY category, supplier_id
)
SELECT category, supplier_id, total_cost_minus_stock_qty
FROM products_stats
WHERE total_cost_minus_stock_qty > 450
ORDER BY category;
