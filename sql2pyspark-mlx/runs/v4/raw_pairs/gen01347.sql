select level, full_name
from employees
where is_remote
union all
select level, full_name
from employees
where email like '%@mail.co.th';
