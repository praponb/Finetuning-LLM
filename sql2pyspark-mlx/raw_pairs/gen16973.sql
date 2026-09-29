select a.id, a.category, a.cost from products a where a.cost < (select avg(b.cost) from products b where b.category = a.category) order by a.category, a.id desc;
