select case when year(hire_date) < 2024 then 'before_2024' when year(hire_date) = 2024 then '2024' else 'after_2024' end as hire_cohort, sum(salary) as total_salary, rank() over (order by sum(salary) desc) as rnk
from employees
where is_remote
group by case when year(hire_date) < 2024 then 'before_2024' when year(hire_date) = 2024 then '2024' else 'after_2024' end
order by rnk desc;
