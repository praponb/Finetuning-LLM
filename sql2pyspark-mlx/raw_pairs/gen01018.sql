SELECT brand, category, COUNT(DISTINCT price) AS unique_brands FROM products GROUP BY brand, category HAVING unique_brands > 1 ORDER BY unique_brands;
