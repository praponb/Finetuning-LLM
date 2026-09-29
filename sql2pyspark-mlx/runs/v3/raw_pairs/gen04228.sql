select id, product_id, unit_price - quantity as line_total
from order_items
where product_id in (12, 23);
