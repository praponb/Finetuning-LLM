SELECT MAX(unit_price) AS max_unit_price, COUNT(*) AS cnt FROM order_items ORDER BY max_unit_price DESC;
