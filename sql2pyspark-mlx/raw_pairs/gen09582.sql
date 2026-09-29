SELECT city, INITCAP(TRIM(country)) AS country_title, ROUND(rating * 100, 2) AS rating_adjusted
FROM suppliers
WHERE country = 'US'
