select id, quantity, unit_price
from order_items
where quantity >= (select avg(quantity) from order_items);
