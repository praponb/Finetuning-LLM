SELECT * FROM (SELECT category, brand, price FROM products)
PIVOT (SUM(price) FOR brand IN ('Umbrella', 'Initech', 'Globex'));
