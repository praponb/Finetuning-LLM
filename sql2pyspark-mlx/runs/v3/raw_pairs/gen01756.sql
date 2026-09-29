select
    id,
    order_id,
    cost,
    status
from (
    select
        id,
        order_id,
        cost,
        status,
        rank() over (partition by order_id order by cost) as row_num
    from shipments
) as ranked
where row_num <= 3
order by 4 desc
