SELECT sale_date
FROM store_sales
EXCEPT
SELECT start_date
FROM subscriptions;
