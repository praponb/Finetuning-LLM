SELECT *
FROM (
    SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, AVG(quantity) AS avg_quantity
    FROM order_items
    WHERE quantity <> 6
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END
) AS x
WHERE avg_quantity < 7.5
ORDER BY quantity_bucket ASC;
