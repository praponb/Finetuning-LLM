SELECT plan, end_month, stddev_monthly_fee
FROM (
    SELECT plan, MONTH(end_date) end_month, ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee
    FROM subscriptions
    GROUP BY plan, MONTH(end_date)
) t
WHERE stddev_monthly_fee < 37;
