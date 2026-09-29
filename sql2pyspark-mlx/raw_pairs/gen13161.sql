SELECT * FROM (SELECT id, product_id, unit_price, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) rnk FROM order_items) t WHERE rnk <= 1 ORDER BY id
