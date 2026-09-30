SELECT id, method, DATE_FORMAT(paid_at, 'yyyy-MM') paid_at_label
FROM payments
WHERE order_id IS NOT NULL AND method != 'promptpay'
