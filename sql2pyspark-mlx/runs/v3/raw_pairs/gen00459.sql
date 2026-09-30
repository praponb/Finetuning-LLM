select dept_name, sort_array(collect_set(location)) as location_list from departments where location not in ('Berlin', 'Singapore') and location = 'Tokyo' group by dept_name;
