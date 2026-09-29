select id, budget, dept_name, location from departments where budget <= (select min(budget) from departments);
