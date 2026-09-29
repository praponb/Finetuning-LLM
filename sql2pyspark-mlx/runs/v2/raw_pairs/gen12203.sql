select budget, dept_name, location, trim(dept_name) clean_dept_name, coalesce(budget, 0) budget_filled from departments
