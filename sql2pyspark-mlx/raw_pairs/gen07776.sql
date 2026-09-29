SELECT id, order_id, unit_price
FROM order_items
WHERE unit_price IS NOT NULL
ORDER BY order_id DESC;
