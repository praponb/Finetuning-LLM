select case when quantity >= 241 then 'high' when quantity >= 97 then 'mid' else 'low' end as quantity_bucket, round(stddev(quantity), 2) as stddev_quantity
from inventory
where store_id in (1, 3, 4, 6) and store_id in (2, 6) and quantity > 192
group by quantity_bucket
having stddev_quantity < 27
order by quantity_bucket;
