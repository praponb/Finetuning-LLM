SELECT quantity FROM order_items WHERE quantity <= 7 UNION ALL SELECT quantity FROM order_items WHERE quantity IS NULL ORDER BY quantity;
