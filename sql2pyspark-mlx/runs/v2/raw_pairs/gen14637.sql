select *, add_months(sale_date, 3) as sale_date_plus_months
from store_sales
where product_id in (1, 6, 8, 9);
