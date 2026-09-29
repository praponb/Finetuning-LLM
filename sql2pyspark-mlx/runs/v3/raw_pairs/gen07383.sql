select store_id, sum(revenue) as total_revenue, sum(units) as total_units
from store_sales
group by store_id
having sum(units) < 110;
