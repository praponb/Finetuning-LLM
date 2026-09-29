select case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end as signup_cohort, sum(age) as total_age, round(sum(age) * 100.0 / sum(sum(age)) over (), 2) as pct_of_total
from customers
where lower(name) like '%avi%'
group by case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end
order by signup_cohort asc
