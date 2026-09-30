SELECT method
FROM payments
WHERE status <> 'SUCCESS' AND method = 'promptpay';
