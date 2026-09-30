select
    id,
    units,
    store_id,
    sale_date,
    units - revenue as revenue_per_unit
from store_sales
where revenue <= 3400 and revenue >= 1300 and units between 12 and 15
order by sale_date asc, id asc
limit 10
