SELECT * FROM (SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, brand, cost FROM products WHERE cost BETWEEN 470 AND 540)
PIVOT (AVG(cost) FOR brand IN ('Acme', 'Globex', 'Umbrella', 'Initech'))
ORDER BY launch_month
