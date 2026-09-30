SELECT quantity, unit_price, CASE WHEN quantity >= 9 THEN 'High' WHEN quantity >= 4 THEN 'Medium' ELSE 'Low' END AS quantity_level
FROM order_items
WHERE quantity <= 4
ORDER BY unit_price DESC;
