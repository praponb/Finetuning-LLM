SELECT o.id, o.status, o.customer_id, s.opened_date, s.region FROM orders o LEFT JOIN stores s ON o.store_id = s.id WHERE s.city = 'Berlin' OR o.amount BETWEEN 770 AND 1550
