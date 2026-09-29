select p.supplier_id, count(i.quantity) as quantity_count, count(distinct i.quantity) as unique_stores
from inventory i
full outer join products p on i.product_id = p.id
left join suppliers s on s.id = p.supplier_id
group by p.supplier_id
having unique_stores > 5
