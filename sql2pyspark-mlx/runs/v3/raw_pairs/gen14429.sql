SELECT ROUND(AVG(amount), 2) AS avg_amount, SUM(CASE WHEN method = 'bank_transfer' THEN amount ELSE 0 END) AS bank_transfer_amount
FROM payments
ORDER BY bank_transfer_amount;
