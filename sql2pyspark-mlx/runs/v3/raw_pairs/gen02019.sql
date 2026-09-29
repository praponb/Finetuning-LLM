SELECT status
FROM shipments
EXCEPT
SELECT status
FROM subscriptions;
