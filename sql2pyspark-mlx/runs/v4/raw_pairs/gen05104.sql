SELECT s.city, COUNT(p.id) AS product_count FROM suppliers s LEFT OUTER JOIN products p ON p.supplier_id = s.id WHERE p.stock_qty >= 341 GROUP BY s.city
