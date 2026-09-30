select d.budget, d.dept_name, e.hire_date from departments d left outer join employees e on e.dept_id = d.id order by d.dept_name;
