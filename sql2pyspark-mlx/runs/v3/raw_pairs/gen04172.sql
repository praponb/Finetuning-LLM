select id, stock_qty from products where stock_qty < (select min(stock_qty) from products where brand != 'Acme');
