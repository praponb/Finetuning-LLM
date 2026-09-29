SELECT oi.id, oi.unit_price FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE order_date BETWEEN '2023-11-23' AND '2024-05-21') ORDER BY unit_price, id;
