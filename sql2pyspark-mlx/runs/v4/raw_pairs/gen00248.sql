select
    store_id,
    quarter(last_restock) last_restock_quarter,
    min(quantity) min_quantity,
    round(avg(quantity), 2) avg_quantity,
    count(*) row_count
from inventory
group by 1, 2
having count(distinct quantity) > 4
order by store_id;
