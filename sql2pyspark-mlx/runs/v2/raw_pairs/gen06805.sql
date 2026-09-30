SELECT *, CASE WHEN plan IN ('premium', 'standard') THEN 'Tier 1' ELSE 'Other' END plan_group
FROM subscriptions
WHERE customer_id IN (12, 24) AND end_date < '2023-06-13'
