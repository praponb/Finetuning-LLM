select order_id, avg(unit_price) as avg_unit_price, max(quantity) - min(quantity) as quantity_range
from order_items
where unit_price <= 350 and (quantity <= 6 or unit_price >= 210)
group by order_id
order by avg_unit_price
