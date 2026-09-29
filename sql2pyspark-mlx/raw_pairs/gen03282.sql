SELECT *
FROM (
    SELECT
        product_id,
        CASE WHEN quantity >= 269 THEN 'high' WHEN quantity >= 112 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        MAX(quantity) - MIN(quantity) AS quantity_range,
        MIN(quantity) AS min_quantity
    FROM inventory
    WHERE quantity >= 96
    GROUP BY product_id, CASE WHEN quantity >= 269 THEN 'high' WHEN quantity >= 112 THEN 'mid' ELSE 'low' END
) AS t
WHERE quantity_range < 53;
