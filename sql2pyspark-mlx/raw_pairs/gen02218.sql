SELECT product_id, quantity * unit_price AS line_total FROM order_items WHERE unit_price BETWEEN 150 AND 310 OR quantity < 8 ORDER BY product_id DESC
