select s.country, s.city, s.supplier_name, p.cost, p.category, p.price from suppliers s left join products p on p.supplier_id = s.id
