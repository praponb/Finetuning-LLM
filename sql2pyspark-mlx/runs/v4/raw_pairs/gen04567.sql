SELECT quantity, product_id, ROUND(quantity * 0.85, 2) AS quantity_scaled, CAST(unit_price AS INT) AS unit_price_int FROM order_items WHERE unit_price BETWEEN 180 AND 400 AND order_id IN (2, 26, 32)
