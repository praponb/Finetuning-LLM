SELECT * FROM (SELECT category, brand, price FROM products)
PIVOT (MAX(price) FOR brand IN ('Initech', 'Globex'))
ORDER BY category;
