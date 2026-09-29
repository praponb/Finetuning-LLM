SELECT status FROM payments WHERE amount < 1420 EXCEPT SELECT status FROM subscriptions WHERE monthly_fee <= 130 ORDER BY status;
