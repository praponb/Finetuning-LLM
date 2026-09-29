SELECT status FROM shipments WHERE status IN ('RETURNED', 'IN_TRANSIT') UNION ALL SELECT status FROM subscriptions WHERE plan IN ('standard', 'basic', 'enterprise') ORDER BY status;
