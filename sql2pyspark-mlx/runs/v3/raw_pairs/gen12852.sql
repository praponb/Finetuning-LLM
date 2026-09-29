select name from customers union all select full_name as name from employees where manager_id in (27, 37, 38) order by name;
