select id, dept_name, budget, row_number() over (partition by dept_name order by budget, id) as row_num from departments where budget between 2471000 and 2786000 order by dept_name, budget desc;
