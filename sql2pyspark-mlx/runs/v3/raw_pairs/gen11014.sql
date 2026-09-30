SELECT revenue AS value
FROM store_sales
UNION
SELECT monthly_fee AS value
FROM subscriptions
WHERE end_date >= '2024-04-01'
ORDER BY value;
