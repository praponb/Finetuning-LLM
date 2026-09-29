select id, budget, location, dept_name, coalesce(budget, 0) as budget_filled from departments where location in ('Bangkok', 'Berlin', 'Chiang Mai') order by dept_name;
