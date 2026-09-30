SELECT unit_price FROM order_items WHERE quantity < 5 AND product_id IS NULL AND quantity >= 6 ORDER BY unit_price;
