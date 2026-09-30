SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, MAX(amount) AS max_amount FROM payments GROUP BY paid_month ORDER BY paid_month
