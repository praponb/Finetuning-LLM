SELECT
    *,
    SUM(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
    AVG(amount) OVER (PARTITION BY status, method ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
    FIRST_VALUE(amount, TRUE) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
FROM payments;
