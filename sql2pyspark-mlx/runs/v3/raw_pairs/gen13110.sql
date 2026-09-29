SELECT id, quantity, unit_price, product_id, ROUND(unit_price / quantity, 2) AS line_total, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity FROM order_items WHERE product_id = 2;
