SELECT CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 100 THEN 'mid' ELSE 'low' END unit_price_bucket, MAX(quantity) max_quantity, SUM(unit_price) total_unit_price
FROM order_items
WHERE product_id IN (19, 38, 40)
GROUP BY CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 100 THEN 'mid' ELSE 'low' END
HAVING SUM(quantity) >= 36;
