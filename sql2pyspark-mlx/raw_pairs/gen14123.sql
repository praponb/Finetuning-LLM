select * from (select *, rank() over (partition by dept_name order by budget desc) as row_num from departments where dept_name = 'Marketing') as r where row_num <= 2;
