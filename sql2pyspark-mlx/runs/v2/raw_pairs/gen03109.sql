select p.id, p.brand, p.category from products p where exists (select 1 from inventory i where i.product_id = p.id and i.store_id in (3, 5))
