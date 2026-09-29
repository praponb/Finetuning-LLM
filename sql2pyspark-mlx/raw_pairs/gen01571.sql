SELECT end_year, plan, monthly_fee_range
FROM (
    SELECT YEAR(end_date) AS end_year, plan, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    WHERE YEAR(start_date) = 2025
    GROUP BY YEAR(end_date), plan
) AS s
WHERE monthly_fee_range > 44
ORDER BY end_year DESC, 2;
