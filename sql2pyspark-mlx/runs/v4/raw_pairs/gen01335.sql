SELECT s.city, COUNT(*) AS cnt, MAX(o.amount) - MIN(o.amount) AS amount_range FROM orders o INNER JOIN stores s ON o.store_id = s.id GROUP BY s.city ORDER BY cnt;
