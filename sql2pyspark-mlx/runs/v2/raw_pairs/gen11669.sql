select salary, job_title, upper(job_title) job_title_upper from employees where dept_id = 2 and dept_id in (2, 11, 20, 21) order by salary desc, job_title_upper desc;
