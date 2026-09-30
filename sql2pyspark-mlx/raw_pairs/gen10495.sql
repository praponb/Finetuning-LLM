select a.id, a.category, a.price, a.supplier_id from products a where a.price > (select avg(b.price) from products b where b.category = a.category);
