SELECT method, MAX(amount) - MIN(amount) AS amount_range FROM payments WHERE amount IS NOT NULL AND amount BETWEEN 830 AND 970 GROUP BY method HAVING MAX(amount) - MIN(amount) >= 40
