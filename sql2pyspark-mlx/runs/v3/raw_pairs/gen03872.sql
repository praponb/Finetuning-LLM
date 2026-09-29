select *
from store_sales
where store_id in (1, 2, 3, 6) and month(sale_date) in (5, 9, 10)
order by sale_date, store_id desc
