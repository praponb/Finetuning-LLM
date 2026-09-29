select case when budget >= 3395000 then 'high' when budget >= 1791000 then 'mid' else 'low' end as budget_bucket, sum(budget) as total_budget, sum(sum(budget)) over (order by case when budget >= 3395000 then 'high' when budget >= 1791000 then 'mid' else 'low' end) as cumulative_budget
from departments
where dept_name = 'Engineering'
group by case when budget >= 3395000 then 'high' when budget >= 1791000 then 'mid' else 'low' end
order by cumulative_budget asc;
