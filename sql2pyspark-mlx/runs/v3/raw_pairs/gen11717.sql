select p.supplier_id, count(i.quantity) as quantity_count, count(distinct i.quantity) as unique_products
from inventory i
right join products p on i.product_id = p.id
where i.store_id in (1, 2, 4)
group by p.supplier_id
order by unique_products;
