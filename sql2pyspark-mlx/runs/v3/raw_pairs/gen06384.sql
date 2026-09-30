select id, email, full_name from employees where salary between 94000 and 134000 union all select id, email, full_name from employees where year(hire_date) >= 2024 order by email desc;
