select d.dept_name, d.budget, e.dept_id from departments d left join employees e on e.dept_id = d.id where e.level not in ('L3', 'L1') and d.location = 'Chiang Mai'
