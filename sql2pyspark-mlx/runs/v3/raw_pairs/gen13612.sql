select case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end as signup_cohort, is_active, count(*) as row_count
from customers
where signup_date <= '2023-03-12' or credit_limit between 6100 and 7200
group by case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end, is_active
