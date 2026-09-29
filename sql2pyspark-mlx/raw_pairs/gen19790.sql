select
    case when budget >= 3650000 then 'LARGE' else 'SMALL' end as budget_tier,
    count(distinct id) as distinct_ids,
    size(collect_set(dept_name)) as num_dept_names,
    count(case when location = 'Chiang Mai' then 1 end) as chiang_mai_count
from departments
group by budget_tier
