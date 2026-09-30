SELECT end_date, plan FROM subscriptions WHERE monthly_fee BETWEEN 70 AND 200 EXCEPT SELECT end_date, plan FROM subscriptions WHERE customer_id IS NULL;
