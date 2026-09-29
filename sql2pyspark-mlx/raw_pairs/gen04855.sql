select s.id, s.city from suppliers s where s.country in ('TH', 'JP') and exists (select 1 from products p where p.supplier_id = s.id);
