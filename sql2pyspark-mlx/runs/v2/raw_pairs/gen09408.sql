select dept_name, max(budget) as max_budget, avg(budget) as avg_budget from departments where location = 'Bangkok' group by dept_name
