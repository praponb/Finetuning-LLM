select p.supplier_id, count(oi.quantity) as quantity_count, count(distinct oi.unit_price) as unique_orders
from order_items oi
left join products p on oi.product_id = p.id
left join suppliers s on p.supplier_id = s.id
group by p.supplier_id
having count(oi.quantity) > 2
order by 2 desc, supplier_id desc;
