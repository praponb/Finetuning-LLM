select brand, category, cost - stock_qty as cost_minus_stock_qty from products where (price <= 1090 and category = 'Books') or category not in ('Home', 'Books', 'Fashion') order by category desc
