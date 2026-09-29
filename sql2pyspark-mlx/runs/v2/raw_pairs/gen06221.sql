select is_remote, sort_array(collect_set(level)) as level_list, max(salary) - min(salary) as salary_range
from employees
where length(email) > 9 and (manager_id is null or dept_id in (28, 30, 34))
group by is_remote
order by salary_range asc
