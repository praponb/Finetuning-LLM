select
    date_trunc('MONTH', last_restock) last_restock_month,
    round(stddev(quantity), 2) stddev_quantity,
    count(*) row_count,
    count(*) inventory_count
from inventory
where month(last_restock) in (4, 9, 12)
group by date_trunc('MONTH', last_restock)
order by row_count
