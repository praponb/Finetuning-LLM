SELECT * FROM (SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, brand, cost FROM products)
PIVOT (COUNT(cost) FOR brand IN ('Stark', 'Acme', 'Initech', 'Umbrella'))
