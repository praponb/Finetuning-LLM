SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    MIN(paid_at) AS started_at,
    COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count,
    GROUPING(DATE_TRUNC('QUARTER', paid_at)) AS grouping_id
FROM payments
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', paid_at), DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('WEEK', paid_at));
