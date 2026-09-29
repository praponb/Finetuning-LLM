SELECT city, region, MIN(opened_date) min_opened_date
FROM stores
WHERE store_name LIKE 'Sia%' AND opened_date BETWEEN '2023-10-01' AND '2024-03-01'
GROUP BY city, region
ORDER BY city, region
LIMIT 5
