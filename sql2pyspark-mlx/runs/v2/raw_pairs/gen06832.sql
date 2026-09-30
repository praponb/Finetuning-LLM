select * from (select is_remote, job_title, salary from employees) pivot (max(salary) for job_title in ('Engineer', 'Designer', 'Analyst'))
