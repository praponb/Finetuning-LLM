select city
from suppliers
where supplier_name like 'E%'
union all
select city
from stores
order by city;
