with top_shipments as (
    select *, row_number() over (partition by order_id order by cost, id) as row_num
    from shipments
    where order_id is null
)
select
    id,
    order_id,
    cost,
    shipped_date
from top_shipments
where row_num <= 5
