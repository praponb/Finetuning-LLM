SELECT end_date event_date FROM subscriptions WHERE customer_id IS NOT NULL EXCEPT SELECT shipped_date event_date FROM shipments ORDER BY event_date;
