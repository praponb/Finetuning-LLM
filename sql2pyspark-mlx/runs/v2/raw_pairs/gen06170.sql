SELECT *
FROM (
    SELECT CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, order_id, MAX(quantity) AS max_quantity
    FROM order_items
    GROUP BY CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END, order_id
) AS sub
WHERE max_quantity >= 5.1;
