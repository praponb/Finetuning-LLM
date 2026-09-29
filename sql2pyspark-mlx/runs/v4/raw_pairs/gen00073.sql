SELECT plan FROM subscriptions WHERE monthly_fee BETWEEN 90 AND 230 UNION SELECT plan FROM subscriptions WHERE plan IN ('enterprise', 'premium', 'standard');
