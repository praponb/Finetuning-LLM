SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount,
    APPROX_PERCENTILE(amount, 0.9) AS amount_p90,
    AVG(amount) AS avg_amount
FROM payments
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('YEAR', paid_at)), (DATE_TRUNC('MONTH', paid_at)));
