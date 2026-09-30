SELECT status FROM payments WHERE method <> 'paypal' INTERSECT SELECT status FROM subscriptions WHERE customer_id IS NULL;
