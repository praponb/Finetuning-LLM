select country label
from suppliers
where country is not null
union all
select full_name label
from employees
where full_name is not null;
