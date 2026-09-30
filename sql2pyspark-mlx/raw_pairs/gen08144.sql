select
    level,
    max(salary) as max_salary,
    concat_ws(', ', sort_array(collect_set(level))) as level_values,
    avg(salary) as avg_salary
from employees
where full_name is null and length(full_name) >= 12 and manager_id is not null
group by level
having sum(case when level = 'L1' then 1 else 0 end) < 58
order by avg_salary desc;
