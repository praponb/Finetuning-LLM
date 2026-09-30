select id, stock_qty, price
from products
where stock_qty >= (select max(stock_qty) from products where brand not in ('Globex', 'Acme'))
order by 2 asc
