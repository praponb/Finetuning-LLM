select p.id, p.brand, s.city, s.supplier_name from products p inner join suppliers s on p.supplier_id = s.id;
