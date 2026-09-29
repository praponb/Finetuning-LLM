SELECT end_date FROM subscriptions WHERE end_date IS NOT NULL INTERSECT SELECT delivered_date FROM shipments WHERE delivered_date IS NOT NULL ORDER BY end_date DESC;
