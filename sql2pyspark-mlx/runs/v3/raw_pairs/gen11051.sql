select o.store_id, count(oi.quantity) as quantity_count
from order_items oi
full outer join products p on p.id = oi.product_id
inner join orders o on oi.order_id = o.id
where p.stock_qty between 195 and 334
group by o.store_id
