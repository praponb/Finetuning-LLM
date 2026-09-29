select product_id, quantity, quarter(last_restock) as last_restock_quarter
from inventory
where product_id = 10;
