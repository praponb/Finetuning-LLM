with order_stats as (
    select order_id, sum(cost) total_cost
    from shipments
    group by order_id
)
select o.id, o.order_date, os.total_cost
from orders o
inner join order_stats os on o.id = os.order_id
order by o.id, order_date
limit 5;
