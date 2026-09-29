select id, full_name from employees where job_title not in ('Analyst', 'Engineer', 'Intern') intersect select id, full_name from employees where hire_date < '2025-10-01' order by full_name
