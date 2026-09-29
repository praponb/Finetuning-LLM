select max(budget) max_budget, count(*) cnt from departments where budget <= 2014000 or dept_name in ('Engineering', 'Sales');
