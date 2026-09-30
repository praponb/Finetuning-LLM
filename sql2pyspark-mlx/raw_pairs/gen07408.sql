WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS pos FROM order_items) SELECT * FROM numbered WHERE pos <= 1;
