select sum(quantity * unit_price) as revenue
from order_items
where quantity >= 8;
