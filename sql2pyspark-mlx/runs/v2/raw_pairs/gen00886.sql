SELECT city, COUNT(*) AS row_count
FROM customers
WHERE city IN ('Tokyo', 'Chiang Mai', 'Berlin')
GROUP BY city
ORDER BY row_count DESC;
