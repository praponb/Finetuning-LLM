select hire_date, coalesce(full_name, 'Unknown') as full_name_filled from employees where salary < 72000 and manager_id = 19 and level = 'L5'
