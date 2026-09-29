SELECT id, quantity, ROUND(quantity / unit_price, 2) AS line_total FROM order_items WHERE unit_price <= 250 AND unit_price BETWEEN 170 AND 300 ORDER BY 1 DESC;
