with recent_suppliers as (
    select *
    from suppliers
    where upper(trim(supplier_name)) = 'PACIFIC WHOLESALE'
),
suppliers_stats as (
    select city, sum(case when country = 'US' then rating else 0 end) as us_rating, round(avg(rating), 2) as avg_rating
    from recent_suppliers
    group by city
)
select *
from suppliers_stats
order by avg_rating asc
