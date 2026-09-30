SELECT status FROM payments WHERE status = 'REFUNDED' INTERSECT SELECT status FROM subscriptions;
