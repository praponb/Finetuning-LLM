select products.id, products.supplier_id from products where products.supplier_id in (select id from suppliers where city in ('London', 'Singapore'))
