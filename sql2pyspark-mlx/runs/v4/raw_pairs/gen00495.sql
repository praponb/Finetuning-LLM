SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS pos FROM order_items) AS ranked WHERE pos <= 3;
