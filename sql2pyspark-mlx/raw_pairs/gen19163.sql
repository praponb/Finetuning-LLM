select
    dept_name,
    min(budget) as min_budget,
    sum(case when dept_name = 'Sales' then budget else 0 end) as sales_budget,
    concat_ws(', ', sort_array(collect_set(location))) as location_values
from departments
where dept_name = 'HR' and location not in ('Bangkok', 'Singapore')
group by dept_name
having min(budget) >= 2257000;
