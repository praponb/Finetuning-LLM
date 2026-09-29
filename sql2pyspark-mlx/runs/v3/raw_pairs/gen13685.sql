select credit_limit_bucket, avg_age
from (
    select case when credit_limit >= 14700 then 'high' when credit_limit >= 5100 then 'mid' else 'low' end as credit_limit_bucket, round(avg(age), 2) as avg_age
    from customers
    group by case when credit_limit >= 14700 then 'high' when credit_limit >= 5100 then 'mid' else 'low' end
) as agg
where avg_age >= 69;
