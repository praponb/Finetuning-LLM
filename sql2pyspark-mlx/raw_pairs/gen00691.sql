SELECT MAX(unit_price) - MIN(unit_price) AS unit_price_range, SUM(quantity) AS total_quantity, AVG(unit_price) AS avg_unit_price FROM order_items;
