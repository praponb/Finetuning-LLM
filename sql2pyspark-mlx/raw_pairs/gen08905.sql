select level
from employees
where manager_id in (24, 40)
union
select level
from employees
where dept_id in (1, 36, 40)
order by level
