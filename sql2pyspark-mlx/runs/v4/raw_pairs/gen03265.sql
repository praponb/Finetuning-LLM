select id, order_id, shipped_date
from (
    select
        id,
        order_id,
        shipped_date,
        row_number() over (partition by order_id order by shipped_date desc, id) pos
    from shipments
) ranked
where pos <= 5;
