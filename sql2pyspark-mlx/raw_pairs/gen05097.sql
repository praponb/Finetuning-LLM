with valid as (
    -- Filter the raw rows and derive helper columns
    select *, lower(dept_name) as dept_name_lower
    from departments
    where dept_name is not null
      and dept_name is distinct from 'Marketing'
)
-- Grouped metrics
select
    case when budget >= 3987000 then 'PREMIUM' else 'STANDARD' end as budget_tier,
    avg(budget) filter (where location = 'Tokyo') as tokyo_avg_budget,
    sort_array(collect_set(location)) as location_list,
    min(budget) as min_budget
from valid
group by case when budget >= 3987000 then 'PREMIUM' else 'STANDARD' end
having tokyo_avg_budget < 1000000
