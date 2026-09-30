SELECT status, MAX(paid_at) AS latest_paid_at FROM payments GROUP BY status ORDER BY latest_paid_at ASC, status DESC LIMIT 3;
