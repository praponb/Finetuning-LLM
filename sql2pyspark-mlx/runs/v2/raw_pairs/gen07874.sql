with order_summary as (
    select order_id, max(cost) as max_cost
    from shipments
    group by order_id
)
select o.discount, o.amount, os.max_cost
from orders o
inner join order_summary os on o.id = os.order_id;
