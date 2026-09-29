select id, amount, order_date
from orders
where amount >= (select avg(amount) from orders)
order by id desc;
