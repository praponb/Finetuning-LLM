SELECT id, quantity, quantity + unit_price AS line_total
FROM order_items
ORDER BY quantity DESC, id DESC;
