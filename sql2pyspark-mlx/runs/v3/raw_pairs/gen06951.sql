select id, unit_price, order_id
from order_items
where unit_price >= (select max(unit_price) from order_items where quantity = 7)
order by unit_price desc, id asc
limit 10
