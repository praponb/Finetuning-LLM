select sale_date
from store_sales
where revenue <= 1100
union
select opened_date
from accounts;
