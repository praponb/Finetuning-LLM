SELECT unit_price AS value, 'order_item' AS source
FROM order_items
UNION
SELECT revenue AS value, 'store_sale' AS source
FROM store_sales
ORDER BY value
