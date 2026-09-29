SELECT method, MIN(paid_at) first_paid_at, COUNT(*) num_payments FROM payments GROUP BY method
