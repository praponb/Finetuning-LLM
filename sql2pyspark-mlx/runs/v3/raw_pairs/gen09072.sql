SELECT opened_date
FROM stores
WHERE sqft = 10040
UNION
SELECT signup_date opened_date
FROM customers
WHERE country = 'JP'
ORDER BY opened_date DESC
