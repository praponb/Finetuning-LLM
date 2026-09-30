SELECT * FROM (SELECT id, order_id, unit_price, quantity, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rn FROM order_items) AS ranked WHERE rn <= 3;
