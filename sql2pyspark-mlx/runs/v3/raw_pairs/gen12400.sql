select s.id, s.region, s.city, s.opened_date from stores s where s.id in (select store_id from orders where order_date <= '2025-05-05') order by city desc, region asc, id asc limit 10
