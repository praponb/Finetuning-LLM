select id, units
from store_sales
where store_id in (2, 3, 5, 6)
order by units desc, id desc
