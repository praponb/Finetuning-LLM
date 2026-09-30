select d.location, count(e.id) as employee_count from departments d left join employees e on e.dept_id = d.id group by d.location order by d.location desc
