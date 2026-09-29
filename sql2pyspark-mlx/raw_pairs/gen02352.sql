SELECT opened_date, region, SUBSTRING(city, 1, 2) AS city_prefix
FROM stores
WHERE city = 'Bangkok' AND opened_date <= '2025-03-01'
