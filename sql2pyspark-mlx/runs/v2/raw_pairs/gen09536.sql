with filtered_stores as (
    select *
    from stores
    where (region <> 'South' and opened_date >= '2024-04-01') or store_name like '%Downt%'
),
Summary as (
    select year(opened_date) as opened_year, min(sqft) as min_sqft
    from filtered_stores
    group by year(opened_date)
)
select opened_year, min_sqft
from Summary
order by opened_year
