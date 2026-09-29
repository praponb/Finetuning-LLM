SELECT id, order_id, unit_price, product_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rn FROM order_items) AS t WHERE rn <= 2 ORDER BY unit_price ASC
