select id, product_id, date_format(last_restock, 'yyyy-MM-dd') as last_restock_label
from inventory
where product_id in (2, 8);
