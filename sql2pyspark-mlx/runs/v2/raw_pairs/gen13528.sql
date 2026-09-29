select
    month(signup_date) signup_month,
    segment,
    round(avg(credit_limit), 2) avg_credit_limit,
    count(distinct credit_limit) unique_citys
from customers
where email like '%@gmail.com' and age <= 67 and upper(name) like '%VY %'
group by month(signup_date), segment
