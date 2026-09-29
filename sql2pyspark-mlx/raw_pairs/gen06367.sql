SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rn FROM order_items WHERE order_id IN (26, 32)) AS r WHERE rn <= 3 ORDER BY quantity;
