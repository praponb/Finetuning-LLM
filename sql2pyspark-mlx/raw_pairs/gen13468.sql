WITH ranked AS (SELECT id, order_id, unit_price, product_id, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rnk FROM order_items) SELECT * FROM ranked WHERE rnk <= 2
