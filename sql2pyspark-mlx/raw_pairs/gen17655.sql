select job_title, 'employee' as kind from employees where full_name like 'Mia%' union all select city, 'customer' as kind from customers where name like 'Pim%'
