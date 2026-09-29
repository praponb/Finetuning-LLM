select signup_date
from customers
where email like '%@outlook.com'
union
select hire_date as signup_date
from employees
where dept_id in (10, 30)
