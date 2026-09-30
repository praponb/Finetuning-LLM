select *, concat(full_name, ', ', level) as full_name_level, case when salary >= 160000 then 'High' when salary >= 42000 then 'Medium' else 'Low' end as salary_level
from employees
where salary between 114000 and 132000 and level in ('L3', 'L4');
