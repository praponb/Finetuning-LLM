select *
from order_items
where not (product_id is not null and (quantity > 4 or product_id = 38));
