SELECT status
FROM orders
EXCEPT
SELECT status
FROM subscriptions
WHERE start_date <= '2025-05-13';
