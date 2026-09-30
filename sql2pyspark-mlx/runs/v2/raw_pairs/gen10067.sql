SELECT * FROM (SELECT supplier_id, brand, cost FROM products)
PIVOT (COUNT(cost) FOR brand IN ('Stark', 'Initech', 'Umbrella'))
