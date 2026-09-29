select p.id, p.category, p.cost, p.stock_qty from products p where p.cost > (select avg(p2.cost) from products p2 where p2.category = p.category) order by p.id
