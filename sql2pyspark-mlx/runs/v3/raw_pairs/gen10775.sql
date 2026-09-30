SELECT opened_date
FROM stores
WHERE sqft != 14064
EXCEPT
SELECT start_date
FROM subscriptions;
