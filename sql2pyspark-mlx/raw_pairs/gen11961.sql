select p.stock_qty, p.category, oi.order_id
from products p
left join order_items oi on p.id = oi.product_id
where oi.order_id in (1, 11, 20, 34)
order by category desc, order_id;
