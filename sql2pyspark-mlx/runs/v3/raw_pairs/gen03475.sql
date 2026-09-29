select p.id, p.stock_qty, s.rating, s.country, s.city from products p join suppliers s on p.supplier_id = s.id where array_contains(p.tags, 'sale') and p.cost <= 390 order by 4 asc;
