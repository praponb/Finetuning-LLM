select a.id, a.category, a.price from products a where a.price = (select min(b.price) from products b where b.category = a.category);
