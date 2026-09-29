SELECT id, order_date, status, amount * shipping_fee AS amount_with_shipping, ROUND(shipping_fee / amount, 2) AS amount_with_shipping_2 FROM orders ORDER BY 5 DESC
