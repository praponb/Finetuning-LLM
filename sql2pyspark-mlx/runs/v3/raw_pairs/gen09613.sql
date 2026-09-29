select dept_name, sum(budget) as total_budget, sum(sum(budget)) over (order by dept_name) as cumulative_budget from departments where budget <= 3719000 group by dept_name
