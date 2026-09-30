select supplier_name, city
from suppliers
where city = 'Singapore'
union all
select supplier_name, city
from suppliers
where country <> 'US';
