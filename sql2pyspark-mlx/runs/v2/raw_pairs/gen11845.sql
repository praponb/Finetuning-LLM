SELECT budget, location, COALESCE(budget, 0) AS budget_filled FROM departments WHERE location IS NOT NULL AND dept_name IN ('HR', 'Marketing', 'Legal')
