select store_id, max(units) as max_units, count(*) as row_count
from store_sales
group by store_id;
