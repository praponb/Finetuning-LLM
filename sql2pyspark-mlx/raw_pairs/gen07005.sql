SELECT id, status, method, CAST(paid_at AS DATE) AS paid_at_day, DATE_TRUNC('WEEK', paid_at) AS paid_at_period FROM payments;
