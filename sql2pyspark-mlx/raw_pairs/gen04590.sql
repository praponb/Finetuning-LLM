select p.supplier_id, p.category, count(oi.quantity) as quantity_count
from order_items oi
left outer join products p on oi.product_id = p.id
inner join suppliers s on s.id = p.supplier_id
group by p.supplier_id, p.category
having quantity_count >= 2
order by p.category, quantity_count
