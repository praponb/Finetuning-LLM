select
    store_id,
    max(units) as max_units,
    count(*) as num_store_sales,
    round(avg(units), 2) as avg_units
from store_sales
where units >= 16 and year(sale_date) >= 2025
group by store_id;
