select
    case when age >= 57 then 'high' when age >= 42 then 'mid' else 'low' end as age_bucket,
    sum(age) as total_age,
    count(*) as cnt,
    rank() over (order by sum(age) desc) as rnk
from customers
where country in ('TH', 'US', 'DE')
group by case when age >= 57 then 'high' when age >= 42 then 'mid' else 'low' end
order by total_age desc;
