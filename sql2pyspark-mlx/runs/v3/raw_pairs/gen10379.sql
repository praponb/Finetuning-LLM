select case when quantity >= 9 then 'high' when quantity >= 2 then 'mid' else 'low' end as quantity_bucket, count(unit_price) as unit_price_count, min(quantity) as min_quantity
from order_items
group by case when quantity >= 9 then 'high' when quantity >= 2 then 'mid' else 'low' end
having count(unit_price) < 1
