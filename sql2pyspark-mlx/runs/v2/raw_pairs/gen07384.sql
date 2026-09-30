SELECT orders.customer_id, orders.shipping_fee, payments.amount FROM orders LEFT OUTER JOIN payments ON payments.order_id = orders.id WHERE payments.status = 'FAILED' AND orders.discount IS NOT NULL
