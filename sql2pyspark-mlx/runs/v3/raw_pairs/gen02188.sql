SELECT city, MAX(opened_date) AS last_opened_date
FROM stores
WHERE (store_name LIKE '%Central%' AND opened_date <= '2024-08-05') OR sqft = 8286
GROUP BY 1
ORDER BY last_opened_date, city
LIMIT 20
