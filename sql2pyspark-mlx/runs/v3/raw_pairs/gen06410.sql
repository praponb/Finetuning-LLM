SELECT merchant, MAX(amount) - MIN(amount) AS amount_range FROM transactions WHERE merchant IS NULL AND amount > 3300 GROUP BY merchant ORDER BY merchant ASC
