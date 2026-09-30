select s.id, s.opened_date from stores s where s.city in ('Bangkok', 'London') and not exists (select 1 from inventory i where i.store_id = s.id and i.store_id in (2, 3, 5));
