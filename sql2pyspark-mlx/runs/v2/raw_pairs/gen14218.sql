select *, cast(salary as int) as salary_int, case when job_title in ('Engineer', 'Analyst') then 'Core' else 'Other' end as job_title_group from employees where job_title in ('Director', 'Manager')
