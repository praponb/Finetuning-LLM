select id, price
from products
where price > (select avg(price) from products where stock_qty >= 219)
order by id desc;
