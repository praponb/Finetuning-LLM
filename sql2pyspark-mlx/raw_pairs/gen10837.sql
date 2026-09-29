with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from orders s
    where exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.cost between 32 and 56
        )
)
select
    id,
    store_id,
    order_date,
    channel,
    discount,
    shipping_fee
from kept
order by shipping_fee;
