select hire_date, year(hire_date) hire_date_year, ceil(salary) salary_whole from employees where manager_id in (13, 15, 28, 37) and manager_id is not null order by hire_date, 2 desc;
