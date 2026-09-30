SELECT * FROM (SELECT category, brand, price FROM products WHERE SIZE(tags) > 2) PIVOT (AVG(price) FOR brand IN ('Initech', 'Globex', 'Acme', 'Stark'));
