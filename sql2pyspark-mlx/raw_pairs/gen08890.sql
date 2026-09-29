with ranked_shipments as (
    select
        id,
        carrier,
        shipped_date,
        status,
        row_number() over (partition by carrier order by shipped_date, id) row_num
    from shipments
)
select
    id,
    carrier,
    shipped_date,
    status
from ranked_shipments
where row_num <= 3;
