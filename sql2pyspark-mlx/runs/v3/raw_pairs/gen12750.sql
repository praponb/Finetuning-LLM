select case when age >= 71 then 'high' when age >= 41 then 'mid' else 'low' end as age_bucket, round(avg(credit_limit), 2) as avg_credit_limit, max(signup_date) as last_signup_date
from customers
group by case when age >= 71 then 'high' when age >= 41 then 'mid' else 'low' end
having round(avg(credit_limit), 2) > 23
