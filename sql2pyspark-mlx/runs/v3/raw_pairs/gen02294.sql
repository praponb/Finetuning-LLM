SELECT status, COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
FROM payments
WHERE method NOT IN ('cash', 'promptpay', 'bank_transfer')
GROUP BY status
ORDER BY failed_count DESC, 1;
