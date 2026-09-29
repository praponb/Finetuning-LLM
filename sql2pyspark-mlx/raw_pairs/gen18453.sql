with numbered as (
    select *, dense_rank() over (partition by store_id order by shipping_fee) as pos
    from orders
)
select
    id,
    store_id,
    shipping_fee,
    channel
from numbered
where pos <= 3;
