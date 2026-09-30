SELECT
    CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END unit_price_bucket,
    product_id,
    COUNT(quantity) quantity_count,
    MAX(unit_price) max_unit_price,
    AVG(quantity) avg_quantity
FROM order_items
WHERE quantity BETWEEN 5 AND 6 OR quantity >= 4
GROUP BY CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END, product_id;
