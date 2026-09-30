select
    quarter(last_restock) as last_restock_quarter,
    product_id,
    max(quantity) as max_quantity,
    avg(quantity) as avg_quantity
from inventory
group by quarter(last_restock), product_id;
