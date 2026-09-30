with numbered as (
    select *, dense_rank() over (partition by store_id order by revenue desc) pos
    from store_sales
)
select
    id,
    store_id,
    revenue,
    units
from numbered
where pos <= 3
order by 4 desc
