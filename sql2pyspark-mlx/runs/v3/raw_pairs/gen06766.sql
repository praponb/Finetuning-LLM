SELECT brand, unique_categorys
FROM (
    SELECT brand, COUNT(DISTINCT cost) AS unique_categorys
    FROM products
    GROUP BY brand
) AS t
WHERE unique_categorys > 6;
