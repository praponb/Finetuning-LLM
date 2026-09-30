select id, stock_qty, supplier_id from products where stock_qty = (select min(stock_qty) from products) order by stock_qty asc;
