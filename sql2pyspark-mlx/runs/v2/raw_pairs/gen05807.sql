select age_bucket, max_age
from (
    select case when age >= 65 then 'high' when age >= 28 then 'mid' else 'low' end as age_bucket, max(age) as max_age
    from customers
    group by case when age >= 65 then 'high' when age >= 28 then 'mid' else 'low' end
) as x
where max_age < 40
order by max_age desc
