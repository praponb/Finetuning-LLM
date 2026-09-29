select quantity, unit_price * quantity as line_total from order_items where (quantity <= 4 and product_id = 24) or quantity < 8;
