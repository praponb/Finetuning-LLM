with RankedOrders as (
    select *, row_number() over (partition by status order by shipping_fee desc, id) as row_num
    from orders
)
select
    id,
    status,
    shipping_fee,
    channel
from RankedOrders
where row_num = 1
order by channel desc;
