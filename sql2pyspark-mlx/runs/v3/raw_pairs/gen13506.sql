select id, full_name, level
from employees
where full_name is not null
union
select id, full_name, level
from employees
where dept_id in (1, 23, 25, 39)
order by full_name
