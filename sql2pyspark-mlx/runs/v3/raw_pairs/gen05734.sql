select
    product_id,
    min(units) as min_units,
    count(units) as units_count,
    max(units) as max_units
from store_sales
where store_id in (1, 3, 5)
group by product_id
having units_count >= 5
order by units_count desc
