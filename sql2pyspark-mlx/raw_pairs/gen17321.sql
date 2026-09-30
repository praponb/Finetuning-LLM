with src as (
    select *, case when carrier = 'Flash' then 1 else 0 end as is_flash
    from shipments
    where year(shipped_date) = 2025 and cost between 36 and 83
),
shipments_stats as (
    select status, max(is_flash) - min(is_flash) as is_flash_range, max(is_flash) as max_is_flash
    from src
    group by status
)
select *
from shipments_stats
order by max_is_flash asc, status asc
limit 10;
