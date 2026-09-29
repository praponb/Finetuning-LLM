select
    departments.id,
    departments.location,
    count(employees.id) as employee_count,
    max(employees.hire_date) as latest_hire_date,
    round(avg(employees.salary), 2) as avg_salary
from departments
left join employees on departments.id = employees.dept_id
where departments.location = 'New York'
group by departments.id, departments.location
having employee_count > 1
