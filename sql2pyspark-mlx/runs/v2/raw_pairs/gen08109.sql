select unit_price
from order_items
where (unit_price <= 350 and order_id = 21) or quantity between 4 and 8;
