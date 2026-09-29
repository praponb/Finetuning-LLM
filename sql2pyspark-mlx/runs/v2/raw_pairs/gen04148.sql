select *
from (
    select
        id,
        order_id,
        shipped_date,
        status,
        dense_rank() over (partition by order_id order by shipped_date desc) as rnk
    from shipments
) as t
where rnk = 1
order by id desc
