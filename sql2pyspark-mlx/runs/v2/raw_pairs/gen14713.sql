select
    store_id,
    avg(units) avg_units,
    round(stddev(revenue), 2) stddev_revenue,
    count(*) total_count
from store_sales
where sale_date between '2023-09-14' and '2025-04-01' or units < 31
group by store_id;
