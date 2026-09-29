SELECT MAX(amount) - MIN(amount) AS amount_range FROM transactions ORDER BY amount_range DESC
