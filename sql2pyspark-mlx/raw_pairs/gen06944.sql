SELECT id, order_id, quantity, LEAST(unit_price, quantity) AS least_unit_price_quantity, ROUND(quantity, 0) AS quantity_rounded FROM order_items;
