select id, salary, level, first_value(level) over (order by salary desc, id) as first_level from employees where email like '%@company.co'
