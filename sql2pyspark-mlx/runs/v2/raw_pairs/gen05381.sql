SELECT plan, COUNT(*) AS num_subscriptions
FROM subscriptions
GROUP BY plan
HAVING num_subscriptions < 1
ORDER BY plan;
