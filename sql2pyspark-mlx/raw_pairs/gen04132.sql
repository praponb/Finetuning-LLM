select
    ss.id,
    ss.revenue,
    ss.product_id,
    ss.store_id
from store_sales ss
where ss.revenue > 2300 and ss.store_id not in (select id from stores where city = 'London')
order by revenue desc;
