SELECT unit_price, product_id, quantity * unit_price AS line_total, FLOOR(unit_price) AS unit_price_whole FROM order_items;
