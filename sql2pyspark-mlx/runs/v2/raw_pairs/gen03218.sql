SELECT city, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
WHERE UPPER(supplier_name) LIKE '%OKY%'
GROUP BY 1
ORDER BY city, avg_rating DESC;
