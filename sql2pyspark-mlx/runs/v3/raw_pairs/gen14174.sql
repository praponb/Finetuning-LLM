select sale_date
from store_sales
intersect
select shipped_date as sale_date
from shipments
order by sale_date desc
