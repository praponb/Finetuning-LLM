with ranked_shipments as (
    select
        id,
        carrier,
        shipped_date,
        order_id,
        dense_rank() over (partition by carrier order by shipped_date) as rnk
    from shipments
    where status = 'DELIVERED'
)
select
    id,
    carrier,
    shipped_date,
    order_id
from ranked_shipments
where rnk <= 3
order by 2;
