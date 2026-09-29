select
    product_id,
    store_id,
    round(avg(revenue), 2) as avg_revenue,
    max(units) - min(units) as units_range
from store_sales
where year(sale_date) >= 2023 and store_id in (2, 3, 6)
group by 1, 2
order by units_range desc, product_id, store_id
limit 5;
