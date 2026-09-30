select
    ss.id,
    ss.store_id,
    ss.revenue,
    ss.product_id
from store_sales ss
where ss.product_id in (select id from products where price < 900)
order by id
limit 5;
