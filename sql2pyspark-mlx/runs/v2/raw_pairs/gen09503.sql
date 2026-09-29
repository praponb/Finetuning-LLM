SELECT order_id, COUNT(*) total_count
FROM order_items
WHERE unit_price <= 340 AND quantity = 5
GROUP BY order_id;
