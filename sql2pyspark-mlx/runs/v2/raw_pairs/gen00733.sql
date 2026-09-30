SELECT product_id, order_id, quantity, ROUND(quantity * 1.15, 2) adjusted_quantity FROM order_items
