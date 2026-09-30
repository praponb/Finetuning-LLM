select name from customers where name is not null except select dept_name from departments where dept_name is not null order by name;
