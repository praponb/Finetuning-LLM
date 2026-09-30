select
    month(launch_date) as launch_month,
    count(cost) as cost_count,
    sum(case when category = 'Sports' then cost else 0 end) as sports_cost,
    sum(case when brand = 'Initech' then 1 else 0 end) as initech_count
from products
where stock_qty <= 339
group by month(launch_date)
order by 1
limit 1;
