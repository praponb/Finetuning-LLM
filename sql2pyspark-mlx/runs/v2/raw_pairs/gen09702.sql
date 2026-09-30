SELECT city, MIN(opened_date) first_opened_date
FROM stores
WHERE city = 'London' AND sqft <> 11779 AND city = 'Singapore'
GROUP BY city
