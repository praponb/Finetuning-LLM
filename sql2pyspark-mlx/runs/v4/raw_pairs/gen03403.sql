SELECT plan FROM subscriptions WHERE NOT (status IN ('ACTIVE', 'EXPIRED', 'CANCELLED') AND customer_id IN (30, 40));
