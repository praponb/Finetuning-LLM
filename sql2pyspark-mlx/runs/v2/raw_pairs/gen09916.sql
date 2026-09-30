select s.id, s.city, s.opened_date from stores s where not exists (select 1 from orders o where o.store_id = s.id and o.channel <> 'store') order by city, id limit 3;
