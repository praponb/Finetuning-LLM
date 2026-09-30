with filtered_employees as (
    select *, upper(job_title) as job_title_upper
    from employees
    where dept_id in (5, 7, 26, 29)
),
summary as (
    select
        case when salary >= 163000 then 'high' when salary >= 45000 then 'mid' else 'low' end as salary_bucket,
        job_title,
        round(stddev(salary), 2) as stddev_salary,
        sum(salary) as total_salary
    from filtered_employees
    group by case when salary >= 163000 then 'high' when salary >= 45000 then 'mid' else 'low' end, job_title
)
select *
from summary
order by 1 desc, stddev_salary;
