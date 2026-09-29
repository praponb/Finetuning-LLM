select id, order_id, cost
from (
    select *, dense_rank() over (partition by order_id order by cost desc) as rn
    from shipments
    where status <> 'DELIVERED'
) as r
where rn <= 3
order by 1 desc
