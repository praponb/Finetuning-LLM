select o.id, o.channel, s.region, s.city from orders o left outer join stores s on o.store_id = s.id where o.amount is not null;
