select
    job_title,
    case when year(hire_date) < 2024 then 'before_2024' when year(hire_date) = 2024 then '2024' else 'after_2024' end as hire_cohort,
    sum(case when job_title = 'Intern' then 1 else 0 end) as intern_count,
    max(hire_date) as latest_hire_date,
    round(stddev(salary), 2) as stddev_salary
from employees
where upper(email) like '%LIC%' and hire_date between '2023-05-20' and '2023-11-01'
group by job_title, case when year(hire_date) < 2024 then 'before_2024' when year(hire_date) = 2024 then '2024' else 'after_2024' end;
