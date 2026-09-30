select
    product_id,
    max(revenue) - min(revenue) revenue_range,
    min(units) min_units,
    round(avg(revenue), 2) avg_revenue
from store_sales
where product_id in (3, 8) and store_id = 2 and units between 28 and 35
group by product_id
order by avg_revenue desc, product_id
