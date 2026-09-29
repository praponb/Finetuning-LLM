select p.brand, count(oi.quantity) as quantity_count
from order_items oi
inner join orders o on o.id = oi.order_id
left join products p on p.id = oi.product_id
group by p.brand
