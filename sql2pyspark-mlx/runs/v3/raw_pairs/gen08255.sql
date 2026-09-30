SELECT id, store_name
FROM stores
WHERE opened_date < '2025-07-01' AND region = 'North'
ORDER BY 1 DESC;
