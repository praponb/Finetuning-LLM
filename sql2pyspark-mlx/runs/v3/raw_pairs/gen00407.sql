select p.price, p.product_name, i.last_restock
from products p
left join inventory i on p.id = i.product_id;
