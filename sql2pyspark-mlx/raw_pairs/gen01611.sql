SELECT
    id,
    plan,
    end_date,
    ROUND(monthly_fee * 1.07, 2) AS adjusted_monthly_fee
FROM subscriptions
WHERE monthly_fee BETWEEN 90 AND 180 AND YEAR(end_date) >= 2023
ORDER BY adjusted_monthly_fee DESC, plan DESC, id
LIMIT 5;
