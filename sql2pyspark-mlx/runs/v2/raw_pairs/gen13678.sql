SELECT id, country, CONCAT(city, ' ', email) city_email
FROM customers
WHERE segment NOT IN ('SMB', 'Corporate', 'Consumer')
