SELECT id, paid_at, amount, COALESCE(amount, 0) amount_filled, DATE_TRUNC('WEEK', paid_at) paid_at_period FROM payments;
