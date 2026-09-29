with ranked_shipments as (
    select
        id,
        carrier,
        cost,
        row_number() over (partition by carrier order by cost desc, id) as rn
    from shipments
    where year(delivered_date) >= 2023
)
select id, carrier, cost
from ranked_shipments
where rn <= 5
order by id
