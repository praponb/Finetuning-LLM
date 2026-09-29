select
    employees.id,
    employees.email,
    employees.level,
    employees.job_title,
    departments.location,
    regexp_extract(employees.email, '@(.+)$', 1) as email_domain
from employees
left join departments on employees.dept_id = departments.id
where departments.dept_name = 'Finance'
order by 1 desc
limit 1
