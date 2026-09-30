select case when quantity >= 206 then 'high' when quantity >= 108 then 'mid' else 'low' end quantity_bucket, round(stddev(quantity), 2) stddev_quantity, min(quantity) min_quantity
from inventory
where quantity between 99 and 165
group by 1
having min(quantity) < 140
