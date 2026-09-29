select * from (select job_title, level, salary from employees)
pivot (max(salary) for level in ('L1', 'L2', 'L5'));
