select a.id, a.category, a.stock_qty, a.product_name from products a where a.stock_qty = (select min(b.stock_qty) from products b where b.category = a.category);
