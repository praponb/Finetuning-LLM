WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rnk FROM order_items) SELECT * FROM numbered WHERE rnk = 1 ORDER BY order_id DESC;
