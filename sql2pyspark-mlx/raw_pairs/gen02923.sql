select a.id, a.category, a.price from products a where a.price > (select avg(b.price) from products b where b.category = a.category) order by a.id desc
