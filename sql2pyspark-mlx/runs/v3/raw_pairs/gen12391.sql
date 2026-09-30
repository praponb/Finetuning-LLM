select case when year(signup_date) < 2024 then 'before_2024' when year(signup_date) = 2024 then '2024' else 'after_2024' end as signup_cohort, segment, sort_array(collect_set(country)) as country_list
from customers
where is_active = true or email like '%@company.co'
group by signup_cohort, segment
order by signup_cohort
