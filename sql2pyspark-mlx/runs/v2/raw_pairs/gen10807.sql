select
    store_id,
    count(case when channel = 'web' then 1 end) as web_count,
    count(*) as total_count,
    sum(case when channel = 'partner' then shipping_fee else 0 end) as partner_shipping_fee
from orders
group by store_id
having count(*) < 3;
