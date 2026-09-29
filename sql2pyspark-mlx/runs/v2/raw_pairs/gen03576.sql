select
    category,
    min(launch_date) as min_launch_date,
    round(avg(price), 2) as avg_price,
    round((100 * sum(case when brand = 'Stark' then 1 else 0 end)) / count(*), 1) as pct_stark
from products
group by category
order by avg_price, category desc
limit 5
