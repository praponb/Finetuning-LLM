select id, stock_qty, product_name from products where stock_qty > (select avg(stock_qty) from products) order by product_name
