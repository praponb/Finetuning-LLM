select order_id, unit_price - quantity line_total
from order_items
where unit_price <= 280
