select *
from (
    select customer_id, store_id, sum(case when channel = 'web' then 1 else 0 end) web_count
    from orders
    group by customer_id, store_id
) t
where web_count >= 29
order by web_count
