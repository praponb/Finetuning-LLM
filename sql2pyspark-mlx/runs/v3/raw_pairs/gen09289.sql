SELECT location, dept_name, COALESCE(budget, 0) budget_filled, COALESCE(dept_name, 'Unknown') dept_name_filled FROM departments WHERE budget >= 2861000 AND budget <= 3986000 ORDER BY location DESC
