select s.city, s.country, s.rating, p.cost, p.supplier_id, p.brand from suppliers s left join products p on s.id = p.supplier_id where s.supplier_name like '%Sia%';
