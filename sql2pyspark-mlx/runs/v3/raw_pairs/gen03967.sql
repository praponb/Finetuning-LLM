SELECT
    end_cohort,
    customer_id,
    pct_cancelled,
    stddev_monthly_fee
FROM (
    SELECT
        CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort,
        customer_id,
        ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cancelled,
        ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM subscriptions
    WHERE YEAR(start_date) >= 2023
    GROUP BY CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END, customer_id
) AS agg
WHERE pct_cancelled >= 55
ORDER BY stddev_monthly_fee DESC, end_cohort, customer_id
LIMIT 1;
