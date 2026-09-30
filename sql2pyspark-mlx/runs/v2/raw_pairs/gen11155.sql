select
    id,
    product_id,
    quantity,
    order_id
from order_items
where unit_price <= 350;
