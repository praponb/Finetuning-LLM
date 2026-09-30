SELECT plan, COUNT(DISTINCT monthly_fee) unique_statuss FROM subscriptions WHERE customer_id IN (13, 30) AND (status IN ('CANCELLED', 'ACTIVE') OR monthly_fee >= 190) GROUP BY plan
