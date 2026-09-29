select avg(unit_price) avg_unit_price, count(quantity) quantity_count, min(quantity) min_quantity
from order_items
where quantity between 6 and 7
