SELECT end_date FROM subscriptions WHERE monthly_fee <= 110 EXCEPT SELECT delivered_date FROM shipments;
