select
    store_id,
    count(quantity) as quantity_count,
    avg(quantity) as avg_quantity,
    min(quantity) as min_quantity
from inventory
where last_restock >= '2025-01-31'
group by 1
order by avg_quantity desc;
