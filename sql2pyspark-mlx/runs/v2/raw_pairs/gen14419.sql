SELECT MONTH(delivered_date) AS delivered_month, COUNT(CASE WHEN carrier = 'Flash' THEN 1 END) AS flash_count, MIN(delivered_date) AS first_delivered_date
FROM shipments
WHERE YEAR(delivered_date) >= 2024
GROUP BY 1
HAVING COUNT(CASE WHEN carrier = 'Flash' THEN 1 END) >= 3
