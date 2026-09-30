select store_id, product_id, max(units) as max_units
from store_sales
where year(sale_date) = 2023 and store_id in (1, 6)
group by store_id, product_id
