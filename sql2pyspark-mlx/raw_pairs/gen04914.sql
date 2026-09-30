SELECT id, product_id, unit_price, RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) rnk FROM order_items WHERE quantity != 5;
