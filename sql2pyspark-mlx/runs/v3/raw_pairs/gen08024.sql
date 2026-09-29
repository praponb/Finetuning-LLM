select * from (select *, row_number() over (partition by dept_id order by salary desc, id) as rn from employees where dept_id in (2, 8, 16, 22)) as r where rn = 1 order by level
