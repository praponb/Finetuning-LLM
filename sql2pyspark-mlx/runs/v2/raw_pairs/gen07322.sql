with ranked_orders as (
    select
        id,
        store_id,
        order_date,
        dense_rank() over (partition by store_id order by order_date) rn
    from orders
)
select id, store_id, order_date
from ranked_orders
where rn <= 3;
