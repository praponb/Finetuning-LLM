SELECT id, product_id, quantity, unit_price * quantity AS line_total, ROUND(quantity / unit_price, 2) AS line_total_2 FROM order_items ORDER BY 1 ASC;
