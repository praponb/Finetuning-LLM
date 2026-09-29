select
    city,
    quarter(opened_date) as opened_quarter,
    region,
    avg(sqft) as avg_sqft,
    sum(case when city = 'New York' then sqft else 0 end) as new_york_sqft
from stores
group by 1, 2, 3
having sum(case when city = 'New York' then sqft else 0 end) < 24
