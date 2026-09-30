select signup_date
from customers
where age <= 50
union
select hire_date as signup_date
from employees
