select country as label
from suppliers
where country is not null
union
select level as label
from employees
where level is not null;
