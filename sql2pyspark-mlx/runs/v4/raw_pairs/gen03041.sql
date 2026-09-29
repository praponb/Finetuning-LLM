select
    location,
    dept_name,
    sum(case when dept_name = 'Sales' then 1 else 0 end) as sales_count,
    min(budget) as min_budget
from departments
group by 1, 2
having min_budget > 3529000;
