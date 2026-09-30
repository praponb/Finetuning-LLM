select
    id,
    carrier,
    delivered_date,
    order_id
from (
    select
        id,
        carrier,
        delivered_date,
        order_id,
        row_number() over (partition by carrier order by delivered_date, id) as rn
    from shipments
) as r
where rn <= 3
order by id, delivered_date desc;
