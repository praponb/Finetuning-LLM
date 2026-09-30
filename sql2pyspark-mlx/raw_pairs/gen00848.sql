select
    case when credit_limit >= 16000 then 'high' when credit_limit >= 7600 then 'mid' else 'low' end as credit_limit_bucket,
    is_active,
    country,
    round(stddev(age), 2) as stddev_age,
    avg(age) as avg_age,
    count(distinct age) as unique_citys
from customers
group by case when credit_limit >= 16000 then 'high' when credit_limit >= 7600 then 'mid' else 'low' end, is_active, country
