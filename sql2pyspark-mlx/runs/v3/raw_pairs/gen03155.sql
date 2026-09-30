select s.id, s.opened_date, s.city from stores s where not exists (select 1 from orders o where o.store_id = s.id) order by id desc
