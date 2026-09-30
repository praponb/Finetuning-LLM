select case when dept_name in ('Finance', 'HR') then 'priority' else 'other' end as dept_name_group, sum(budget) as total_budget, rank() over (order by sum(budget) desc) as budget_rank
from departments
group by case when dept_name in ('Finance', 'HR') then 'priority' else 'other' end
order by dept_name_group;
