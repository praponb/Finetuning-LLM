select s.id, s.city, s.opened_date, s.region from stores s where s.city in ('Bangkok', 'Singapore', 'London') and exists (select 1 from orders o where o.store_id = s.id);
