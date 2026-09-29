SELECT id, product_id, unit_price, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price) AS dense_rnk FROM order_items WHERE unit_price BETWEEN 290 AND 340
