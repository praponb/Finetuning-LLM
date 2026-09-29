select level, manager_id, sort_array(collect_set(level)) as level_list, count(case when level = 'L1' then 1 end) as l1_count from employees group by level, manager_id
