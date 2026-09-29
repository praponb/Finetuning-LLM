SELECT *, cost / NULLIF(stock_qty, 0) cost_per_stock_qty, DATEDIFF(CURRENT_DATE(), launch_date) days_since_launch_date
FROM products
WHERE supplier_id = 25
ORDER BY id DESC;
