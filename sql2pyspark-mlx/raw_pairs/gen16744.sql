select p.category, p.supplier_id, max(i.quantity) as max_quantity
from inventory i
left outer join products p on i.product_id = p.id
inner join stores s on i.store_id = s.id
group by p.category, p.supplier_id;
