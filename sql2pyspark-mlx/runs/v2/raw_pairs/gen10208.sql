select e.id, e.job_title, e.email, e.salary from employees e where e.full_name not like '%Carol%' and e.dept_id in (select id from departments where dept_name is not null) order by salary desc;
