select full_name as label
from employees
where full_name is not null
except
select city as label
from suppliers
where city is not null
