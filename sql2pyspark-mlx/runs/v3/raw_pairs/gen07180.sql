with store_stats as (
    select store_id, min(amount) as min_amount, count(*) as num_orders
    from orders
    group by store_id
)
select s.store_name, ss.min_amount, ss.num_orders
from stores s
left join store_stats ss on s.id = ss.store_id
order by min_amount;
