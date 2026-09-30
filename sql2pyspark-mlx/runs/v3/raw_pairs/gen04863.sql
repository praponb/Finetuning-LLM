select s.id, s.opened_date, s.region from stores s where s.id in (select store_id from orders) order by s.id desc;
