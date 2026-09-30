SELECT MONTH(s.opened_date) AS opened_month, s.region, AVG(i.quantity) AS avg_quantity
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
GROUP BY MONTH(s.opened_date), s.region
HAVING AVG(i.quantity) >= 120
ORDER BY 2 ASC;
