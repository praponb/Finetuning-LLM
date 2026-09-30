SELECT * FROM (SELECT supplier_id, category, stock_qty FROM products WHERE price <= 990)
PIVOT (COUNT(stock_qty) FOR category IN ('Fashion', 'Electronics', 'Home', 'Grocery'))
ORDER BY supplier_id;
