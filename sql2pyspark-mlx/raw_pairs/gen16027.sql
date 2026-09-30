select
    country,
    sum(case when city = 'Bangkok' then 1 else 0 end) as bangkok_count,
    round(stddev(age), 2) as stddev_age,
    count(distinct credit_limit) as unique_countrys
from customers
where signup_date <= '2024-10-01'
group by country
having count(*) >= 2
