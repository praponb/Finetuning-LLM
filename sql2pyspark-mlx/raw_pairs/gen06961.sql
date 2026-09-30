SELECT status, MAX(paid_at) AS latest_paid_at, MAX(amount) - MIN(amount) AS amount_range, MIN(amount) AS min_amount FROM payments GROUP BY status HAVING MAX(amount) - MIN(amount) >= 31;
