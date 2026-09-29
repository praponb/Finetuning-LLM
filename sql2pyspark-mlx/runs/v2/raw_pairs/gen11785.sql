SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, COUNT(sqft) AS sqft_count
FROM stores
WHERE sqft <= 11230 AND opened_date BETWEEN '2023-03-03' AND '2024-02-01'
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
HAVING MIN(sqft) < 10000
ORDER BY opened_month ASC, sqft_count ASC;
