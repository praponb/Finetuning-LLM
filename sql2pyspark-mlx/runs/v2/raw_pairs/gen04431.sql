SELECT id, method, status FROM payments WHERE (status = 'FAILED' AND method != 'promptpay') OR TO_DATE(paid_at) < '2023-12-07';
