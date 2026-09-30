select p.category, p.product_name, s.rating from products p join suppliers s on p.supplier_id = s.id;
