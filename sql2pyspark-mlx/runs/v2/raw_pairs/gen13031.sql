WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) row_num FROM order_items) SELECT * FROM numbered WHERE row_num <= 2 ORDER BY unit_price DESC, order_id DESC
