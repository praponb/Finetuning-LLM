with ranked_orders as (
    select
        id,
        customer_id,
        discount,
        shipping_fee,
        row_number() over (partition by customer_id order by discount, id) as rn
    from orders
)
select *
from ranked_orders
where rn <= 3
