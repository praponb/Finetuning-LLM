SELECT *, LENGTH(status) status_length
FROM subscriptions
WHERE NOT (plan = 'standard' AND plan = 'basic')
