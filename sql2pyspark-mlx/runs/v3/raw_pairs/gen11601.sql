select
    dept_id,
    case when level in ('L4', 'L5') then 'group_a' else 'other' end as level_group,
    sum(salary) as total_salary,
    min(hire_date) as min_hire_date
from employees
group by rollup(dept_id, case when level in ('L4', 'L5') then 'group_a' else 'other' end)
order by level_group asc;
