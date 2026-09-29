with numbered as (
    select *, dense_rank() over (partition by carrier order by cost) pos
    from shipments
    where status in ('RETURNED', 'LOST', 'DELIVERED')
)
select
    id,
    carrier,
    cost,
    delivered_date
from numbered
where pos = 1
order by delivered_date;
