SELECT ROUND(AVG(unit_price), 2) AS avg_unit_price, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items;
