with ranked as (select *, row_number() over (partition by job_title order by salary desc, id) as rn from employees) select id, job_title, salary, dept_id from ranked where rn <= 1
