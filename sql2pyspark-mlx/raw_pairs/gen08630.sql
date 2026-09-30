SELECT DATE_TRUNC('MONTH', products.launch_date) AS launch_month, MIN(order_items.quantity) AS min_quantity
FROM order_items
LEFT JOIN products ON order_items.product_id = products.id
GROUP BY DATE_TRUNC('MONTH', products.launch_date)
ORDER BY 1 DESC, 2 DESC
