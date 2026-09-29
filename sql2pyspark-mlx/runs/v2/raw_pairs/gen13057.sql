SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS row_num FROM order_items) AS t WHERE row_num <= 1
