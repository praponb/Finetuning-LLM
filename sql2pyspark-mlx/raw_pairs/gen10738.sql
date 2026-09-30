SELECT order_id, MIN(quantity) min_quantity
FROM order_items
WHERE unit_price >= 340 AND quantity <= 8
GROUP BY 1
ORDER BY min_quantity;
