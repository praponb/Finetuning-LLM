select i.id, i.store_id, p.category
from inventory i
left join products p on i.product_id = p.id
where i.last_restock > '2024-11-03';
