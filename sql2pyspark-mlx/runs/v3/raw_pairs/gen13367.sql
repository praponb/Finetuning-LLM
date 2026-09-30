select s.id, s.country, s.supplier_name from suppliers s where exists (select 1 from products p where p.supplier_id = s.id and p.price between 350 and 1120);
