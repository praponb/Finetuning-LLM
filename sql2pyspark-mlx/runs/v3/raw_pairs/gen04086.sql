select departments.location, concat_ws(', ', sort_array(collect_set(employees.level))) as level_values
from employees
inner join departments on employees.dept_id = departments.id
group by departments.location
order by level_values desc, location
limit 5;
