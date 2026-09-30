select d.id, d.budget, d.location from departments d where d.budget is not null and exists (select 1 from employees e where e.dept_id = d.id and e.salary between 74000 and 121000);
