SELECT status, 'order' AS source FROM orders WHERE shipping_fee BETWEEN 14 AND 40 UNION ALL SELECT status, 'subscription' AS source FROM subscriptions WHERE customer_id IN (8, 25, 36) ORDER BY status
