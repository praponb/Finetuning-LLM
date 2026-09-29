SELECT o.id, o.status, s.city, s.opened_date FROM orders o FULL OUTER JOIN stores s ON o.store_id = s.id WHERE UPPER(TRIM(s.store_name)) = 'OLD TOWN' ORDER BY id DESC
