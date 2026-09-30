SELECT id, order_id, unit_price FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS pos FROM order_items WHERE order_id IN (2, 17, 25, 38)) AS t WHERE pos <= 5;
