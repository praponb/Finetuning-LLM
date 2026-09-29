SELECT city, REPLACE(email, ' ', '') AS email_slug, TRIM(segment) AS clean_segment
FROM customers
WHERE country = 'DE' AND city = 'Chiang Mai'
