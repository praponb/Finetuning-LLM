SELECT product_id, quantity, ROUND(unit_price / quantity, 2) line_total
FROM order_items
WHERE unit_price >= 150;
