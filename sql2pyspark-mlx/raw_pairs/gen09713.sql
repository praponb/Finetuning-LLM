SELECT delivered_date FROM shipments WHERE MONTH(delivered_date) IN (1, 4, 7) INTERSECT SELECT end_date FROM subscriptions WHERE plan = 'basic';
