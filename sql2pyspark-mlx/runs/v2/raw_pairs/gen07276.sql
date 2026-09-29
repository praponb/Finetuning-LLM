select a.id, a.category, a.stock_qty from products a where a.stock_qty < (select avg(b.stock_qty) from products b where b.category = a.category);
