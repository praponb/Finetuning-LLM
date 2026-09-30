SELECT *, CASE WHEN unit_price >= 310 THEN 'High' WHEN unit_price >= 130 THEN 'Medium' ELSE 'Low' END AS unit_price_tier, unit_price * quantity AS line_total
FROM order_items
WHERE unit_price IS NOT NULL AND (unit_price < 240 OR product_id IN (11, 28));
