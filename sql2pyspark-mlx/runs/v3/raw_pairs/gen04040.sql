select o.id, o.discount, o.amount, s.city, s.store_name from orders o right join stores s on o.store_id = s.id where s.region = 'North' and year(s.opened_date) >= 2024;
