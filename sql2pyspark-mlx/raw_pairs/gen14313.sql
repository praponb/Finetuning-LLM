SELECT *
FROM (
    SELECT CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, MIN(unit_price) AS min_unit_price, MIN(quantity) AS min_quantity
    FROM order_items
    WHERE product_id IS NULL
    GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
) AS sub
WHERE min_quantity < 3.4;
