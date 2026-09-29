with ranked as (
    select *, row_number() over (partition by order_id order by delivered_date, id) as row_num
    from shipments
)
select id, order_id, delivered_date
from ranked
where row_num <= 3
