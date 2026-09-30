select id, city, supplier_name
from suppliers
where rating between 3.8 and 3.9
union
select id, city, supplier_name
from suppliers
where supplier_name like '%Siam%'
