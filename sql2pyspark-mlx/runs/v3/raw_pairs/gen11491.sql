select store_id, max(sale_date) latest_sale_date
from store_sales
where store_id in (2, 3, 5)
group by store_id
