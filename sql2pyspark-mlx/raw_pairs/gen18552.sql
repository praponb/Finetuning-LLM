SELECT price AS value
FROM products
UNION ALL
SELECT unit_price AS value
FROM order_items
ORDER BY value DESC
