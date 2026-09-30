SELECT method, SUM(CASE WHEN method = 'card' THEN amount ELSE 0 END) AS card_amount
FROM payments
GROUP BY method
