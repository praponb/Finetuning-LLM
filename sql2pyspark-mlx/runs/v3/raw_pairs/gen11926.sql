select case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end as signup_cohort, month(signup_date) as signup_month, sum(case when city = 'New York' then credit_limit else 0 end) as new_york_credit_limit
from customers
where name is not null and segment = 'Consumer'
group by case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end, month(signup_date)
order by new_york_credit_limit
