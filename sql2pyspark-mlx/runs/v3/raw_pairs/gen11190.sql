select id, job_title, salary, row_number() over (partition by job_title order by salary, id) rn from employees where dept_id in (7, 20, 26, 27)
