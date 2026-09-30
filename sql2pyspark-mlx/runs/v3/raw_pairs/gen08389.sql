select store_id, min(units) as min_units, min(sale_date) as min_sale_date
from store_sales
where units <= 38 and sale_date < '2023-12-01'
group by store_id
order by min_units, store_id
limit 50;
