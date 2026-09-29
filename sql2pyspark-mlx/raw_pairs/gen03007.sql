SELECT o.id, o.discount, o.channel, o.amount, s.city, s.store_name, s.region, DAY(s.opened_date) opened_date_day FROM orders o RIGHT JOIN stores s ON o.store_id = s.id
