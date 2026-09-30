SELECT method, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at, DATE_TRUNC('week', paid_at) AS paid_at_period FROM payments WHERE status = 'REFUNDED' ORDER BY method
