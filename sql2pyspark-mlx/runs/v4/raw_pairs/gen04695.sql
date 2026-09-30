SELECT o.amount, s.opened_date, s.store_name
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
ORDER BY store_name, opened_date
