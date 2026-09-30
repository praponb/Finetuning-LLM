select *, trim(email) as clean_email, date_format(hire_date, 'yyyy-MM') as hire_date_label
from employees
where salary > 77000 and hire_date <= '2023-09-01'
