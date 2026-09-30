select dept_name, sort_array(collect_set(dept_name)) as dept_name_list
from departments
group by dept_name
order by dept_name;
