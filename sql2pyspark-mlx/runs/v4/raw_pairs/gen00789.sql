SELECT id, unit_price, RANK() OVER (ORDER BY unit_price DESC) AS unit_price_rank FROM order_items WHERE product_id IN (20, 29) ORDER BY unit_price DESC
