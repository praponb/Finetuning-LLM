select d.dept_name, d.budget, e.salary, e.dept_id, e.email from departments d left join employees e on d.id = e.dept_id order by e.salary, email;
