with ranked as (
    select
        id,
        carrier,
        shipped_date,
        status,
        row_number() over (partition by carrier order by shipped_date desc, id) row_num
    from shipments
)
select *
from ranked
where row_num <= 1
order by row_num asc;
