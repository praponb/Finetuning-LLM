select case when quantity >= 9 then 'high' when quantity >= 4 then 'mid' else 'low' end as quantity_bucket, round(stddev(quantity), 2) as stddev_quantity
from order_items
where (product_id in (9, 10, 13, 30) and order_id in (13, 30, 39)) or product_id = 38
group by case when quantity >= 9 then 'high' when quantity >= 4 then 'mid' else 'low' end
