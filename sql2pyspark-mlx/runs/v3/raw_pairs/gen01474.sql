select p.brand, round(stddev(i.quantity), 2) as stddev_quantity
from inventory i
right join products p on p.id = i.product_id
left join stores s on s.id = i.store_id
where s.region is not null
group by p.brand
order by stddev_quantity desc
