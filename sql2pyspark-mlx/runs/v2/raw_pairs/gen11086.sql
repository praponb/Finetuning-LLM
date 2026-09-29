SELECT status, method FROM payments WHERE method != 'promptpay' UNION ALL SELECT status, method FROM payments WHERE method IN ('bank_transfer', 'paypal') ORDER BY status;
