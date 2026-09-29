SELECT brand, initech_count, unique_categorys
FROM (
    SELECT brand, COUNT(CASE WHEN brand = 'Initech' THEN 1 END) AS initech_count, COUNT(DISTINCT stock_qty) AS unique_categorys
    FROM products
    GROUP BY brand
) AS x
WHERE unique_categorys >= 9
