select s.id, s.supplier_name from suppliers s where s.country in ('SG', 'TH') and not exists (select 1 from products p where p.supplier_id = s.id);
