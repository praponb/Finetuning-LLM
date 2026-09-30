select
    product_id,
    store_id,
    round(avg(revenue), 2) as avg_revenue,
    max(units) - min(units) as units_range,
    count(distinct revenue) as unique_stores
from store_sales
where product_id in (2, 4)
group by product_id, store_id
order by units_range desc, product_id, store_id
limit 5;
