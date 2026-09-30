select stores.region, count(*) as cnt
from inventory
left join products on inventory.product_id = products.id
join stores on inventory.store_id = stores.id
group by stores.region;
