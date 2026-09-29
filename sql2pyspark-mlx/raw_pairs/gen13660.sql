select hire_date, 'employee' as kind from employees where month(hire_date) in (4, 9, 12) union select opened_date, 'store' as kind from stores where region = 'Central' order by hire_date;
