SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC) AS rn FROM order_items) AS r WHERE rn <= 3 ORDER BY unit_price DESC;
