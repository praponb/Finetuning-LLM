SELECT ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE NOT (status IN ('REFUNDED', 'FAILED', 'SUCCESS') AND method IN ('paypal', 'cash', 'bank_transfer'))
ORDER BY stddev_amount
