select p.brand, sum(i.quantity) as total_quantity
from inventory i
left outer join products p on i.product_id = p.id
group by p.brand;
