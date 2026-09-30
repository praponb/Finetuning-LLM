SELECT id, store_name
FROM stores
WHERE city != 'London'
UNION ALL
SELECT id, store_name
FROM stores
WHERE opened_date BETWEEN '2023-12-24' AND '2024-09-28'
ORDER BY id
