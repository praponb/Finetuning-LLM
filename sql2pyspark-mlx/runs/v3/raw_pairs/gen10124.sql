SELECT opened_date
FROM stores
UNION ALL
SELECT end_date AS opened_date
FROM subscriptions
ORDER BY opened_date DESC
