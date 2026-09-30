SELECT start_date AS event_date
FROM subscriptions
INTERSECT
SELECT shipped_date AS event_date
FROM shipments;
