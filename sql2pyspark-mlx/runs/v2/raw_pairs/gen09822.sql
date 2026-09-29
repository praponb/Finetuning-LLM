SELECT unit_price, CEIL(unit_price) AS unit_price_whole FROM order_items WHERE order_id IN (20, 24, 34) AND quantity IS NOT NULL;
