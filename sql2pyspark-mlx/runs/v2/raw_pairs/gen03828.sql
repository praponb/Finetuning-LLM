select id, quantity
from inventory
where quantity > (select max(quantity) from inventory)
order by id desc
limit 10
