select p.id, p.supplier_id, p.price from products p where month(p.launch_date) in (9, 10, 11) and exists (select 1 from inventory i where i.product_id = p.id) order by price desc, id asc limit 1;
