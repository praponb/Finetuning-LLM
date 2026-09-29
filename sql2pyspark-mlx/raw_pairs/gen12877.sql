SELECT *
FROM (
    SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, product_id, MAX(quantity) AS max_quantity
    FROM order_items
    WHERE quantity BETWEEN 3 AND 5
    GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END, product_id
) AS sub
WHERE max_quantity >= 5.4
