SELECT *, LEAST(unit_price, quantity) least_unit_price_quantity, LEAST(quantity, unit_price) least_quantity_unit_price FROM order_items ORDER BY quantity DESC
