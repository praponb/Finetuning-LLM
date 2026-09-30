select p.supplier_id, sum(oi.quantity) as total_quantity, round(avg(oi.quantity), 2) as avg_quantity
from order_items oi
inner join products p on oi.product_id = p.id
group by p.supplier_id
order by total_quantity desc
