select
    store_id,
    round(avg(revenue), 2) as avg_revenue,
    count(distinct units) as unique_stores,
    max(units) as max_units
from store_sales
group by store_id
