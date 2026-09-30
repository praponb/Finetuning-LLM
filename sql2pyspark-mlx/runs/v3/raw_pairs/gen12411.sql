SELECT unit_price, product_id, quantity + unit_price AS line_total
FROM order_items
WHERE unit_price < 170 AND unit_price >= 270
ORDER BY line_total DESC;
