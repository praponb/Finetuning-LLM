SELECT *
FROM (
    SELECT DATE_TRUNC('MONTH', paid_at) AS paid_month, COUNT(*) AS row_count
    FROM payments
    GROUP BY DATE_TRUNC('MONTH', paid_at)
) AS agg
WHERE row_count >= 3;
