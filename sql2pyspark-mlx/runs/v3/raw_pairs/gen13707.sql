SELECT status, customer_id FROM subscriptions WHERE monthly_fee >= 200 AND (customer_id IS NOT NULL OR monthly_fee >= 130)
