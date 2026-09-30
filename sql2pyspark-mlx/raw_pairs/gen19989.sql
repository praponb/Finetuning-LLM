select id, product_id, last_day(last_restock) as last_restock_month_end
from inventory
where (quantity != 145 and last_restock >= '2023-04-30') or store_id = 6;
