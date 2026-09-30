SELECT
    id,
    paid_at,
    status,
    amount,
    MAX(amount) OVER (PARTITION BY DATE_TRUNC('month', paid_at), status) AS paid_at_period_max_amount
FROM payments;
