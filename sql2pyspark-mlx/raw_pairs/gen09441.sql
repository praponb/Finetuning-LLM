select supplier_name
from suppliers
where city = 'Berlin'
union
select supplier_name
from suppliers
where country <> 'SG'
order by supplier_name
