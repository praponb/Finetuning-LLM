SELECT *, ROUND(unit_price / quantity, 2) line_total, quantity + unit_price line_total_2
FROM order_items
WHERE product_id IN (18, 31, 33, 36)
ORDER BY unit_price DESC, quantity;
