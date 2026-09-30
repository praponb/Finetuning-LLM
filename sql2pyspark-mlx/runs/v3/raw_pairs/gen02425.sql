SELECT MAX(amount) AS max_amount, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count
FROM payments
WHERE method IN ('bank_transfer', 'paypal') AND (status <> 'REFUNDED' OR order_id = 7);
