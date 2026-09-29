select avg(sqft) as avg_sqft
from stores
where city = 'London' or opened_date > '2023-10-01'
order by avg_sqft
