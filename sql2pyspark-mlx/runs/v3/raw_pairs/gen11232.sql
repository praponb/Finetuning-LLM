select id, full_name, hire_date, case when job_title in ('Director', 'Manager') then 'Core' else 'Other' end as job_title_group, upper(level) as level_upper from employees
