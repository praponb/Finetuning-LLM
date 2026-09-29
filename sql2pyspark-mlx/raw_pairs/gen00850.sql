select * from (select *, rank() over (partition by level order by salary) as row_num from employees) as t where row_num <= 2 order by hire_date, dept_id
