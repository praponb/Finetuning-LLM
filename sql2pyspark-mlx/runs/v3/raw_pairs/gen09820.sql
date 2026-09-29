select p.id, p.category, p.cost, p.supplier_id from products p where p.cost = (select min(p2.cost) from products p2 where p2.category = p.category)
