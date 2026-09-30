SELECT unit_price, FLOOR(unit_price) unit_price_whole FROM order_items WHERE order_id IN (4, 7, 8, 11);
