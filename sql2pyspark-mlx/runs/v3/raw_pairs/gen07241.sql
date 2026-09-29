SELECT method, ROUND(amount, 0) AS amount_rounded
FROM payments
WHERE amount > 1120 AND method = 'bank_transfer'
