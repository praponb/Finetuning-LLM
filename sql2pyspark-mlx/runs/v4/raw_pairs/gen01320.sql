select hire_date, 'employee' as source from employees union select opened_date, 'store' as source from stores;
