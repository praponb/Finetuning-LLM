select p.id, p.product_name from products p where p.supplier_id not in (select id from suppliers where supplier_name like '%Toky%');
