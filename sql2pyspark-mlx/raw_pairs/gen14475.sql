with top_employees as (select *, row_number() over (partition by level order by hire_date desc, id) as row_num from employees) select * from top_employees where row_num <= 5
