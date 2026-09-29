select o.id, o.channel, o.store_id, s.opened_date from orders o inner join stores s on o.store_id = s.id where o.amount > 1470 or s.store_name is not null;
