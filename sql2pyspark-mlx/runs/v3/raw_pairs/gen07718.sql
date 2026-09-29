select
    store_id,
    sum(units) as total_units,
    count(*) as cnt,
    sum(sum(units)) over (order by store_id) as cumulative_units
from store_sales
group by store_id
order by cumulative_units desc
