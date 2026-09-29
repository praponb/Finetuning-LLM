SELECT brand, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, COUNT(DISTINCT cost) AS unique_suppliers
FROM products
WHERE price BETWEEN 630 AND 920
GROUP BY brand
