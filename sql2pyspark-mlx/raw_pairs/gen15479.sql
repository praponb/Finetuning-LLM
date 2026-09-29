with src as (
    select *, upper(trim(supplier_name)) as supplier_name_normalized
    from suppliers
    where city in ('Singapore', 'Chiang Mai') and (rating <= 2.8 or rating < 3.8)
),
summary as (
    select city, round((100 * sum(case when city = 'London' then 1 else 0 end)) / count(*), 1) as pct_london, count(*) as row_count
    from src
    group by city
)
select city, row_count
from summary;
