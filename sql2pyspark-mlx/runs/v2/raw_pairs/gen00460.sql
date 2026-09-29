select dept_name, sum(budget) as total_budget from departments where location not in ('Berlin', 'Singapore') group by dept_name order by dept_name, total_budget desc;
