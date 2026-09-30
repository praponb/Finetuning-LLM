select s.region, round(stddev(i.quantity), 2) as stddev_quantity, min(i.quantity) as min_quantity
from inventory i
right join products p on p.id = i.product_id
join stores s on i.store_id = s.id
group by s.region
order by stddev_quantity, s.region desc;
