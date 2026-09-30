select sale_date
from store_sales
where units between 21 and 31
union
select sale_date
from store_sales
where sale_date between '2025-01-06' and '2025-03-05';
