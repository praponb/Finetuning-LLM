SELECT id, city
FROM stores
WHERE MONTH(opened_date) IN (4, 5, 6)
