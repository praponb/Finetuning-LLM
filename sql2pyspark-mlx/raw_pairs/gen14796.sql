select i.id, i.store_id, p.price, s.country from inventory i join products p on i.product_id = p.id join suppliers s on p.supplier_id = s.id where p.category = 'Home' and s.supplier_name is not null;
