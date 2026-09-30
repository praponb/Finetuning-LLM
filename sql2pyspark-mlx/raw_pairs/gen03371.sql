select i.quantity, i.last_restock, p.supplier_id
from inventory i
left join products p on i.product_id = p.id
order by quantity desc, last_restock desc
