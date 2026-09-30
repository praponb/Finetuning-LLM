SELECT QUARTER(paid_at) AS paid_quarter, status, MAX(amount) - MIN(amount) AS amount_range
FROM payments
WHERE amount > 1330 AND (status = 'SUCCESS' OR method = 'paypal')
GROUP BY QUARTER(paid_at), status
