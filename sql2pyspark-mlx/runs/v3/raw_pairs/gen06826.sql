select level, job_title from employees where is_remote and full_name like '%Jac%' and hire_date <= '2024-10-01' order by job_title desc;
