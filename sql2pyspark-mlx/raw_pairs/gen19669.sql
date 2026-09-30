SELECT customer_id, status, TRIM(status) clean_status
FROM subscriptions
WHERE status = 'ACTIVE' OR monthly_fee BETWEEN 130 AND 140;
