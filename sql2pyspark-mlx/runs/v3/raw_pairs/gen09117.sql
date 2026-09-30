select
    segment,
    year(signup_date) as signup_year,
    max(age) - min(age) as age_range,
    count(distinct credit_limit) as unique_countrys
from customers
where credit_limit is null
group by segment, signup_year
having max(age) - min(age) >= 26
order by signup_year;
