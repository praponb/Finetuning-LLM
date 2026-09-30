select
    case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end as signup_cohort,
    sum(age) as total_age,
    count(*) as cnt,
    dense_rank() over (order by sum(age) desc) as age_rank
from customers
where city = 'New York'
group by case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end
order by total_age, signup_cohort
limit 10
