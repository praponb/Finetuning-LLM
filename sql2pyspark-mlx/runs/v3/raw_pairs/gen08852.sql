select d.dept_name, e.full_name, e.job_title, e.hire_date from departments d left join employees e on e.dept_id = d.id where e.full_name not like 'b%' order by dept_name, e.job_title desc;
