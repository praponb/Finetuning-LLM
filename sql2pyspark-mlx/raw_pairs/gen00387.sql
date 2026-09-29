SELECT x.status, y.plan FROM (SELECT DISTINCT status FROM shipments) x CROSS JOIN (SELECT DISTINCT plan FROM subscriptions) y ORDER BY status, plan;
