SELECT MAX(amount) - MIN(amount) amount_range, MAX(paid_at) latest_paid_at FROM payments;
