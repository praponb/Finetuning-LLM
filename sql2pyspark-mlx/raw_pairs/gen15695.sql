with enriched as (
    select
        id,
        dept_name,
        location,
        min(budget) over w_ordered as running_min_budget,
        sum(budget) over w_ordered as running_sum_budget,
        max(budget) over w_ordered as running_max_budget,
        cume_dist() over w_part as budget_cume_dist
    from departments
    window w_ordered as (partition by dept_name, location order by budget asc, id asc rows between unbounded preceding and current row), w_part as (partition by dept_name, location order by budget desc)
)
select
    id,
    dept_name,
    location,
    case when location rlike '^(Singapore|Berlin)$' then 'GROUP_A' when location rlike '^(Bangkok|Chiang Mai)$' then 'PRIORITY' else 'CORE' end as location_zone
from enriched
order by location asc nulls last, dept_name desc
