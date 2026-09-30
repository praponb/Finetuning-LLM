select count(*) as order_item_count, count(unit_price) as unit_price_count, min(unit_price) as min_unit_price
from order_items
where quantity = 5 or unit_price >= 160
