SELECT city, MAX(rating) - MIN(rating) AS rating_range, COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
FROM suppliers
WHERE city NOT IN ('Tokyo', 'Bangkok') AND supplier_name LIKE 'Gl%' AND country IN ('DE', 'SG')
GROUP BY 1
HAVING MAX(rating) - MIN(rating) > 37
ORDER BY uk_count DESC, 2 DESC
