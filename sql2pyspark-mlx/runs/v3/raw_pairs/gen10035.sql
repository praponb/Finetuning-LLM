SELECT id, monthly_fee, LOWER(plan) AS plan_lower
FROM subscriptions
WHERE monthly_fee BETWEEN 120 AND 190;
