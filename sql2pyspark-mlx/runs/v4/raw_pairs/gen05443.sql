select e.full_name as employee_full_name, m.salary as manager_salary, m.full_name as manager_full_name
from employees e
left join employees m on e.manager_id = m.id
where e.manager_id in (16, 28) and e.email is not null
order by employee_full_name desc;
