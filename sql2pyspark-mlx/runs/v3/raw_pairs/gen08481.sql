SELECT quantity_bucket, product_id, unique_products
FROM (
    SELECT CASE WHEN quantity >= 208 THEN 'high' WHEN quantity >= 88 THEN 'mid' ELSE 'low' END AS quantity_bucket, product_id, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    GROUP BY CASE WHEN quantity >= 208 THEN 'high' WHEN quantity >= 88 THEN 'mid' ELSE 'low' END, product_id
) AS t
WHERE unique_products < 9;
