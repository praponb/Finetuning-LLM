select id, dept_name, budget, cast(budget as int) budget_int from departments where budget is null and location = 'Bangkok' and location = 'Tokyo' order by budget, dept_name desc
