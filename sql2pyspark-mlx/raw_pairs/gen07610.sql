SELECT payments.id, payments.amount, payments.status FROM payments WHERE payments.order_id IN (SELECT id FROM orders WHERE discount <= 0.4) ORDER BY payments.amount ASC
