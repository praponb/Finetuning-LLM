select level, manager_id, round((100 * sum(case when level = 'L5' then 1 else 0 end)) / count(*), 1) as pct_l5
from employees
where manager_id in (13, 22, 24, 38) and full_name like 'Ka%' and lower(trim(full_name)) = 'david lee'
group by level, manager_id
order by level
