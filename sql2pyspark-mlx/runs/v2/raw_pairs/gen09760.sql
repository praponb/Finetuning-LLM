SELECT plan, CASE WHEN plan IN ('premium', 'basic') THEN 'Core' ELSE 'Other' END plan_group
FROM subscriptions
WHERE status IN ('CANCELLED', 'ACTIVE') AND start_date <= '2024-12-01' AND customer_id IN (10, 11, 17, 30);
