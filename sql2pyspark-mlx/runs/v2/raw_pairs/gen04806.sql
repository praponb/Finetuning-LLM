select id, level, hire_date from (select id, level, hire_date, row_number() over (partition by level order by hire_date, id) as pos from employees) as r where pos <= 1
