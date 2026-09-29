SELECT *
FROM (
    SELECT
        product_id,
        CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 5 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        COUNT(DISTINCT quantity) AS unique_products,
        AVG(unit_price) AS avg_unit_price
    FROM order_items
    WHERE quantity = 4
    GROUP BY product_id, CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 5 THEN 'mid' ELSE 'low' END
) AS s
WHERE unique_products > 8
ORDER BY avg_unit_price DESC;
