SELECT *, ROUND(quantity / unit_price, 2) AS line_total
FROM order_items
WHERE product_id IN (26, 35)
ORDER BY product_id DESC, line_total DESC
