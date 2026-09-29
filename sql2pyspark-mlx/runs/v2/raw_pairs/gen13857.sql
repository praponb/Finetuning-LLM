SELECT id, product_id, LEAST(unit_price, quantity) least_unit_price_quantity FROM order_items
