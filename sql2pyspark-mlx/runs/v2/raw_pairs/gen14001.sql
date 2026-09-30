select
    id,
    units,
    revenue,
    store_id
from store_sales
where units <= (select min(units) from store_sales where revenue > 3800)
