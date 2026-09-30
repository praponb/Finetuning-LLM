select s.city, s.country, s.supplier_name, p.cost from suppliers s left join products p on p.supplier_id = s.id where s.city is not null;
