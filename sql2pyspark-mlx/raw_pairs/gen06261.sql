SELECT ROUND(AVG(amount), 2) AS avg_amount, MAX(amount) - MIN(amount) AS amount_range FROM payments WHERE method = 'promptpay' ORDER BY amount_range DESC
