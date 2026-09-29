select p.id, p.category, p.price, p.brand, s.country from products p inner join suppliers s on p.supplier_id = s.id order by p.id asc;
