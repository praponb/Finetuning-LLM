SELECT brand, SUM(CASE WHEN brand = 'Stark' THEN price ELSE 0 END) AS stark_price
FROM products
WHERE ARRAY_CONTAINS(tags, 'new')
GROUP BY brand
HAVING SUM(CASE WHEN brand = 'Stark' THEN price ELSE 0 END) > 65
