select s.id, s.city, s.region, s.opened_date from stores s where s.store_name like 'Ai%' and not exists (select 1 from orders o where o.store_id = s.id and year(o.order_date) = 2023)
