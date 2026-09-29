SELECT end_date
FROM subscriptions
UNION ALL
SELECT opened_date AS end_date
FROM stores
ORDER BY end_date;
