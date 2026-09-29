select full_name
from employees
where hire_date between '2023-06-01' and '2023-10-21'
intersect
select full_name
from employees
where email like '%@startup.io'
