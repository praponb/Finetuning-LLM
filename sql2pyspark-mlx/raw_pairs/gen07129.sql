SELECT id, product_id, unit_price FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS rn FROM order_items) AS ranked WHERE rn <= 3;
