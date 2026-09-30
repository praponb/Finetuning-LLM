SELECT sale_date
FROM store_sales
WHERE units <= 13
EXCEPT
SELECT end_date
FROM subscriptions
WHERE end_date > '2025-05-12'
