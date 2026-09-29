select case when unit_price >= 390 then 'high' when unit_price >= 80 then 'mid' else 'low' end as unit_price_bucket, round(stddev(quantity), 2) as stddev_quantity
from order_items
group by 1
order by stddev_quantity desc
