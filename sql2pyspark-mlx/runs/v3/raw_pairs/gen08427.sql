SELECT customer_id, TRIM(plan) clean_plan FROM subscriptions WHERE monthly_fee BETWEEN 80 AND 200;
