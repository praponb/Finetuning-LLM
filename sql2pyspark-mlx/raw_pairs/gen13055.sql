SELECT AVG(unit_price) AS avg_unit_price, ROUND(AVG(quantity), 2) AS avg_quantity
FROM order_items
