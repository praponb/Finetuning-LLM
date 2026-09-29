select
    product_id,
    count(distinct revenue) as unique_products,
    max(units) as max_units,
    count(units) as units_count
from store_sales
where units < 22
group by product_id
having max(units) > 15
order by max_units desc;
