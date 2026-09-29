SELECT id, status, order_id
FROM payments
WHERE method IN ('bank_transfer', 'promptpay')
UNION
SELECT id, status, order_id
FROM payments
WHERE amount > 1120;
