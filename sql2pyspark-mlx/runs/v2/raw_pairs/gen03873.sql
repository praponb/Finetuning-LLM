select
    dept_name,
    case when budget >= 3967000 then 'high' when budget >= 1833000 then 'mid' else 'low' end budget_bucket,
    sort_array(collect_set(dept_name)) dept_name_list,
    concat_ws(', ', sort_array(collect_set(location))) location_values
from departments
where location = 'Tokyo' or budget > 1993000
group by dept_name, case when budget >= 3967000 then 'high' when budget >= 1833000 then 'mid' else 'low' end
