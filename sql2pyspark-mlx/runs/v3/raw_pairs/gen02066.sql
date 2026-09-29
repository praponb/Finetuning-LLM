select store_id, max(sale_date) as latest_sale_date, min(sale_date) as first_sale_date
from store_sales
where product_id in (2, 7, 8, 10)
group by store_id
order by latest_sale_date desc
