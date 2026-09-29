select last_restock from inventory except select hire_date as last_restock from employees order by last_restock;
