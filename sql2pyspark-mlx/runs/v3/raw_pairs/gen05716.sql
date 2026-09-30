SELECT brand, SUM(price) AS total_price, COUNT(DISTINCT price) AS unique_brands FROM products WHERE brand IS NOT NULL AND cost BETWEEN 270 AND 450 GROUP BY brand ORDER BY total_price DESC;
