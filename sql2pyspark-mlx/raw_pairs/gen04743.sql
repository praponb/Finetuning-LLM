SELECT * FROM (SELECT category, brand, price FROM products WHERE launch_date BETWEEN '2025-04-04' AND '2025-05-01')
PIVOT (SUM(price) FOR brand IN ('Initech', 'Stark', 'Acme', 'Umbrella'))
ORDER BY category;
