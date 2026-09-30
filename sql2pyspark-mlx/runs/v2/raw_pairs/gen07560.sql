select id, amount, discount
from orders
where amount > (select avg(amount) from orders)
order by amount desc, id desc
limit 3
