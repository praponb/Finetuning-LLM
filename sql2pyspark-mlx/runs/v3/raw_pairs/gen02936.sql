SELECT COUNT(quantity) AS quantity_count
FROM order_items
WHERE quantity <= 8 AND unit_price >= 250;
