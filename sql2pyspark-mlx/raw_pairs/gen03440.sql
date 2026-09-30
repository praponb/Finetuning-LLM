SELECT
    QUARTER(end_date) AS end_quarter,
    COUNT(monthly_fee) AS monthly_fee_count,
    MAX(monthly_fee) AS max_monthly_fee,
    MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE customer_id = 40 AND status IN ('CANCELLED', 'EXPIRED') AND YEAR(end_date) >= 2025
GROUP BY QUARTER(end_date)
ORDER BY end_quarter DESC;
