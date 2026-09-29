SELECT amount
FROM transactions
INTERSECT
SELECT monthly_fee AS amount
FROM subscriptions
ORDER BY amount
