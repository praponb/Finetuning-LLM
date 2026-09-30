SELECT product_id, COALESCE(quantity, 0) AS quantity_filled FROM order_items WHERE quantity BETWEEN 6 AND 8
