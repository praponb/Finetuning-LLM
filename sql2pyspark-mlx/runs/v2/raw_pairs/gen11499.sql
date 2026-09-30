select count(distinct quantity) as unique_products, max(quantity) - min(quantity) as quantity_range
from order_items
where quantity > 3 and quantity between 4 and 8
order by quantity_range;
