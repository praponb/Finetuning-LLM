select city, age_count, credit_limit_range
from (
    select city, count(age) as age_count, max(credit_limit) - min(credit_limit) as credit_limit_range
    from customers
    group by city
) as s
where age_count >= 7
order by city asc
limit 20;
