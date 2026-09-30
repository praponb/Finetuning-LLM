select
    case when budget >= 3393000 then 'high' when budget >= 1579000 then 'mid' else 'low' end as budget_bucket,
    location,
    sort_array(collect_set(dept_name)) as dept_name_list,
    sort_array(collect_set(location)) as location_list
from departments
group by 1, 2;
