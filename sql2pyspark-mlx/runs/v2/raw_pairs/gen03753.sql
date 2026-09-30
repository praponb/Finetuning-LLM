SELECT AVG(quantity) AS avg_quantity, SUM(unit_price * quantity) AS revenue, AVG(unit_price) AS avg_unit_price
FROM order_items
ORDER BY avg_unit_price DESC, revenue DESC;
