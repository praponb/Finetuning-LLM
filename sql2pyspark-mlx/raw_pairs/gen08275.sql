select *
from suppliers
where (rating <= 2.6 and length(supplier_name) < 13) or city in ('Bangkok', 'Berlin', 'New York')
order by id desc;
