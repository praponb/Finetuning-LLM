SELECT end_date
FROM subscriptions
WHERE monthly_fee < 170
UNION ALL
SELECT delivered_date
FROM shipments
ORDER BY end_date;
