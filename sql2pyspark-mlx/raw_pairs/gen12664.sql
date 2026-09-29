SELECT i.store_id, s.opened_date, s.city
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
WHERE NOT (s.city = 'Tokyo' AND s.opened_date BETWEEN '2023-08-27' AND '2024-05-23')
