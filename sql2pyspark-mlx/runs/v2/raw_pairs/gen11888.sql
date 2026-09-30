SELECT *, DATE_TRUNC('WEEK', paid_at) AS paid_at_period FROM payments WHERE order_id IN (8, 30, 31, 34);
