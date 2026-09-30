SELECT quantity, product_id
FROM order_items
WHERE order_id = 5
UNION ALL
SELECT quantity, product_id
FROM order_items
WHERE unit_price IS NOT NULL
ORDER BY product_id DESC
