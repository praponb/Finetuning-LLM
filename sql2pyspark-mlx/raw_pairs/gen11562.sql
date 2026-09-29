select
    product_id,
    store_id,
    month(last_restock) as last_restock_month,
    round(stddev(quantity), 2) as stddev_quantity
from inventory
group by product_id, store_id, month(last_restock)
order by last_restock_month desc;
