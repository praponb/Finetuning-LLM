SELECT *, FLOOR(unit_price) AS unit_price_whole
FROM order_items
WHERE product_id IN (23, 29)
ORDER BY id DESC
