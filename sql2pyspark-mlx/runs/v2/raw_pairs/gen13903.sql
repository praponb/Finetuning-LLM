SELECT s.store_name, o.order_date, o.amount FROM stores s JOIN orders o ON s.id = o.store_id WHERE s.opened_date > '2023-03-01' AND s.city = 'London';
