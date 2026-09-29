select revenue
from store_sales
union
select cost as revenue
from shipments
order by revenue;
