SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS rn FROM order_items) AS ranked WHERE rn <= 3 ORDER BY rn;
