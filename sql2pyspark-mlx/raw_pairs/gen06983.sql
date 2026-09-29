SELECT sale_date
FROM store_sales
WHERE revenue >= 3400
EXCEPT
SELECT end_date
FROM subscriptions
WHERE plan NOT IN ('basic', 'standard');
