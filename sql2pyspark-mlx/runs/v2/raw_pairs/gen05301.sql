select p.id, p.supplier_id, s.city from products p inner join suppliers s on s.id = p.supplier_id where length(p.product_name) >= 5 order by id;
