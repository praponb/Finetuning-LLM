SELECT *, DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label FROM payments WHERE order_id IN (12, 26) OR method NOT IN ('promptpay', 'paypal', 'card') ORDER BY id DESC, 3 LIMIT 3;
