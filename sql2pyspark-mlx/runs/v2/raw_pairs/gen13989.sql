SELECT
    id,
    status,
    end_date,
    customer_id
FROM subscriptions
WHERE status NOT IN ('PAUSED', 'ACTIVE', 'EXPIRED') AND (plan = 'enterprise' OR status = 'CANCELLED')
ORDER BY status ASC;
