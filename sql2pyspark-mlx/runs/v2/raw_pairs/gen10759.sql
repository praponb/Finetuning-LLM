SELECT x.carrier, y.plan FROM (SELECT DISTINCT carrier FROM shipments) x CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) y ORDER BY carrier, plan
