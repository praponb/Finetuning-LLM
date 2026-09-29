select id, amount
from orders
where amount < (select min(amount) from orders)
