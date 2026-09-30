SELECT amount, 'transaction' source FROM transactions UNION SELECT shipping_fee amount, 'order' source FROM orders ORDER BY source
