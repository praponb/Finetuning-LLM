SELECT email AS label
FROM customers
WHERE city = 'London'
INTERSECT
SELECT country AS label
FROM suppliers
WHERE country IN ('UK', 'TH', 'US')
ORDER BY label DESC
